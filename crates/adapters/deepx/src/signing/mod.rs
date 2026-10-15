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
use parity_scale_codec::Decode;
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
    client::ClientState,
    config::{
        DefaultExtrinsicParamsBuilder, ExtrinsicParams, ExtrinsicParamsEncoder, Hasher,
        TransactionExtension, substrate::BlakeTwo256, transaction_extensions,
    },
    dynamic::Value,
    error::ExtrinsicParamsError,
    tx,
    utils::{AccountId20, H256, Static},
};
use subxt_signer::eth::{Keypair, Signature};
use thiserror::Error;

use crate::common::DeepXPrivateKey;

/// DeepX runtime types required for AccountId20 Ethereum-compatible signatures.
#[derive(Clone, Copy, Debug)]
pub enum DeepXRuntimeConfig {}

#[doc(hidden)]
#[derive(Debug)]
pub struct CheckNonceEra {
    genesis_hash: H256,
}

#[doc(hidden)]
#[derive(Debug, Decode)]
pub enum DeepXNonceEra {
    Immortal,
    Mortal(u64),
}

impl ExtrinsicParams<DeepXRuntimeConfig> for CheckNonceEra {
    type Params = ();

    fn new(client: &ClientState<DeepXRuntimeConfig>, (): ()) -> Result<Self, ExtrinsicParamsError> {
        Ok(Self {
            genesis_hash: client.genesis_hash,
        })
    }
}

impl ExtrinsicParamsEncoder for CheckNonceEra {
    fn encode_value_to(&self, bytes: &mut Vec<u8>) {
        // The captured CheckNonceEra extra encodes Immortal as variant index zero
        bytes.push(0);
    }

    fn encode_implicit_to(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.genesis_hash.0);
    }
}

impl TransactionExtension<DeepXRuntimeConfig> for CheckNonceEra {
    type Decoded = Static<DeepXNonceEra>;

    fn matches(identifier: &str, _: u32, _: &scale_info::PortableRegistry) -> bool {
        identifier == "CheckNonceEra"
    }
}

impl Config for DeepXRuntimeConfig {
    type AccountId = AccountId20;
    type Address = AccountId20;
    type Signature = Signature;
    type Hasher = BlakeTwo256;
    type Header = subxt_core::config::substrate::SubstrateHeader<u32, Self::Hasher>;
    type ExtrinsicParams = transaction_extensions::AnyOf<
        Self,
        (
            transaction_extensions::VerifySignature<Self>,
            transaction_extensions::CheckSpecVersion,
            transaction_extensions::CheckTxVersion,
            transaction_extensions::CheckNonce,
            transaction_extensions::CheckGenesis<Self>,
            transaction_extensions::CheckMortality<Self>,
            transaction_extensions::ChargeAssetTxPayment<Self>,
            transaction_extensions::ChargeTransactionPayment,
            transaction_extensions::CheckMetadataHash,
            CheckNonceEra,
        ),
    >;
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

fn immortal_params(nonce: u64) -> subxt_core::config::ParamsFor<DeepXRuntimeConfig> {
    let (
        signature,
        spec,
        tx_version,
        nonce_param,
        genesis,
        mortality,
        asset_payment,
        payment,
        metadata_hash,
    ) = DefaultExtrinsicParamsBuilder::<DeepXRuntimeConfig>::new()
        .nonce(nonce)
        .immortal()
        .build();
    (
        signature,
        spec,
        tx_version,
        nonce_param,
        genesis,
        mortality,
        asset_payment,
        payment,
        metadata_hash,
        (),
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
    sign_dynamic_pallet_call_with_runtime(
        snapshot.identity(),
        snapshot.interfaces(),
        snapshot.client_state(),
        key,
        pallet,
        call,
        arguments,
        nonce,
    )
}

fn sign_dynamic_pallet_call_with_runtime(
    identity: &ApprovedRuntimeIdentity,
    interfaces: &DeepXRuntimeInterfaceCatalog,
    client_state: &ClientState<DeepXRuntimeConfig>,
    key: &DeepXPrivateKey,
    pallet: &str,
    call: &str,
    arguments: Vec<Value>,
    nonce: u64,
) -> Result<SignedPalletExtrinsic, SigningError> {
    interfaces.call(pallet, call)?;

    let signer = Keypair::from_secret_key(*key.as_bytes()).map_err(|_| SigningError::InvalidKey)?;
    let account_id = derive_signer_account_id(key)?;
    let payload = subxt_core::dynamic::tx(pallet, call, arguments);
    let transaction =
        tx::create_v4_signed(&payload, client_state, immortal_params(nonce))?.sign(&signer);
    let hash = transaction.hash_with(BlakeTwo256);

    Ok(SignedPalletExtrinsic {
        bytes: transaction.into_encoded(),
        extrinsic_hash: hash.0,
        signer: account_id,
        nonce,
        runtime: identity.clone(),
    })
}

#[cfg(test)]
mod tests {
    use nautilus_core::hex;
    use rstest::rstest;
    use serde::Deserialize;

    use super::*;
    use crate::common::{DeepXEnvironment, DeepXKeyScheme};

    const SPEC376_METADATA: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test_data/runtime/testnet/",
        "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
    ));

    #[derive(Deserialize)]
    struct RpcResponse {
        result: String,
    }

    fn runtime() -> (
        DeepXReadOnlyRuntimeSnapshot,
        ClientState<DeepXRuntimeConfig>,
    ) {
        let response: RpcResponse = serde_json::from_str(SPEC376_METADATA).unwrap();
        let metadata = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let snapshot = DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap(),
            376,
            1,
            &metadata,
        )
        .unwrap();
        let client_state = ClientState::<DeepXRuntimeConfig> {
            metadata: snapshot.metadata().clone(),
            genesis_hash: H256::from(snapshot.identity().genesis_hash),
            runtime_version: subxt_core::client::RuntimeVersion {
                spec_version: snapshot.identity().spec_version,
                transaction_version: snapshot.identity().transaction_version,
            },
        };
        (snapshot, client_state)
    }

    fn key() -> DeepXPrivateKey {
        DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap()
    }

    fn sign(
        snapshot: &DeepXReadOnlyRuntimeSnapshot,
        client_state: &ClientState<DeepXRuntimeConfig>,
        pallet: &str,
        call: &str,
        arguments: Vec<Value>,
        nonce: u64,
    ) -> SignedPalletExtrinsic {
        sign_dynamic_pallet_call_with_runtime(
            snapshot.identity(),
            snapshot.interfaces(),
            client_state,
            &key(),
            pallet,
            call,
            arguments,
            nonce,
        )
        .unwrap()
    }

    fn encode_call(
        snapshot: &DeepXReadOnlyRuntimeSnapshot,
        pallet: &str,
        call: &str,
        arguments: Vec<Value>,
    ) -> Vec<u8> {
        let payload = subxt_core::dynamic::tx(pallet, call, arguments);
        tx::payload::Payload::encode_call_data(&payload, snapshot.metadata()).unwrap()
    }

    #[rstest]
    fn spec376_immortal_extrinsics_match_pinned_sdk() {
        let (snapshot, client_state) = runtime();
        let cases = [
            (
                "Subaccount",
                "no_op",
                Vec::new(),
                1_725_000_000_124,
                "850184fcad0b19bb29d4674531d6f115237e16afce377ccd76f6b9583b6dfd829576bc6888f00fa985b12eb69488d9215ad76133b8792113e341e9f2dc4a0e073af90495f8926c9d4c9e817f29ee384e83fb3dd18d78f600000b7c2203a2910100131c",
            ),
            (
                "System",
                "remark",
                vec![Value::from_bytes(b"deepx-offline-signing-check")],
                1_725_000_000_123,
                "f50184fcad0b19bb29d4674531d6f115237e16afce377cdd2abfbe77653eff8459e3105b30feb8295341991ff6cd015b5a94ca0ae27bf30f2be47c10c8a8f926f0ba538bd72d5ddb9a6eb38549961dfb0fa8e96433bc6f01000b7b2203a291010000006c64656570782d6f66666c696e652d7369676e696e672d636865636b",
            ),
            (
                "PerpMarket",
                "place_order",
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
                }
                .into_dynamic_arguments(),
                1_725_000_000_125,
                "b90284fcad0b19bb29d4674531d6f115237e16afce377c6476e0a03c23f5e2d94326d35a5d1db1a3b03e19e7270548a4781c5ffcd9c63f13f98e79b13089f1010035607eff80c9990aa7dcbbeb35fee0fb7bae24df0b2401000b7d2203a291010016021111111111111111111111111111111111111111070001ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff000001ffffffffffffffffffffffffffffffff000000",
            ),
        ];

        for (pallet, call, arguments, nonce, expected) in cases {
            let signed = sign(&snapshot, &client_state, pallet, call, arguments, nonce);
            assert_eq!(signed.bytes(), hex::decode(expected).unwrap());
            assert!(signed.has_valid_hash());
            assert_eq!(signed.runtime(), snapshot.identity());
        }
    }

    #[rstest]
    #[case("Subaccount", "initialize_subaccount", 19, 0, &["name"])]
    #[case("Subaccount", "delete_subaccount", 19, 1, &["subaccount"])]
    #[case("Subaccount", "set_delegate_account", 19, 2, &["delegate", "name", "valid_until"])]
    #[case("Subaccount", "update_delegate_mode", 19, 4, &["address", "new_mode"])]
    #[case("Subaccount", "set_spot_margin", 19, 6, &["address", "enable_spot_margin"])]
    #[case("Subaccount", "rename_subaccount", 19, 7, &["subaccount", "new_name"])]
    #[case("Subaccount", "no_op", 19, 28, &[])]
    #[case("Subaccount", "remove_delegate_account", 19, 29, &["delegate"])]
    #[case("SpotMarket", "place_order", 20, 1, &["params"])]
    #[case("SpotMarket", "cancel_order", 20, 3, &["params"])]
    #[case("PerpMarket", "place_order", 22, 2, &["params"])]
    #[case("PerpMarket", "cancel_order", 22, 3, &["params"])]
    #[case("PerpMarket", "set_profit_and_loss_point", 22, 13, &["subaccount", "market_id", "take_profit_point", "stop_loss_point"])]
    #[case("PerpMarket", "close_position", 22, 14, &["subaccount", "market_id", "price", "slippage"])]
    #[case("PerpMarket", "set_global_leverage", 22, 43, &["subaccount", "max_leverage"])]
    #[case("PerpMarket", "set_per_market_leverage", 22, 44, &["subaccount", "market_id", "max_leverage"])]
    #[case("Lending", "buy_quota", 24, 12, &["address", "quota", "from_subaccount"])]
    fn spec376_metadata_call_contracts(
        #[case] pallet_name: &str,
        #[case] call_name: &str,
        #[case] pallet_index: u8,
        #[case] call_index: u8,
        #[case] fields: &[&str],
    ) {
        let (snapshot, _) = runtime();
        let pallet = snapshot.metadata().pallet_by_name(pallet_name).unwrap();
        let call = pallet.call_variant_by_name(call_name).unwrap();

        assert_eq!(pallet.index(), pallet_index);
        assert_eq!(call.index, call_index);
        assert_eq!(
            call.fields
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            fields,
        );
        snapshot.interfaces().call(pallet_name, call_name).unwrap();
    }

    #[rstest]
    fn spec376_perp_place_scale_encoding_is_exact() {
        let (snapshot, client_state) = runtime();
        let params = DeepXPerpPlaceParams {
            subaccount: [0x11; 20],
            market_id: 7,
            is_long: true,
            size: 123,
            price: 456,
            order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Ioc),
            take_profit: Some(789),
            stop_loss: None,
            reduce_only: true,
            post_only: DeepXPostOnlyParam::MustPostOnly,
        };
        let mut expected = vec![22, 2];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&params.market_id.to_le_bytes());
        expected.push(1);
        expected.extend_from_slice(&params.size.to_le_bytes());
        expected.extend_from_slice(&params.price.to_le_bytes());
        expected.extend_from_slice(&[0, 1, 1]);
        expected.extend_from_slice(&789_u128.to_le_bytes());
        expected.extend_from_slice(&[0, 1, 1]);

        let arguments = params.into_dynamic_arguments();
        assert_eq!(
            encode_call(&snapshot, "PerpMarket", "place_order", arguments.clone()),
            expected,
        );
        let signed = sign(
            &snapshot,
            &client_state,
            "PerpMarket",
            "place_order",
            arguments,
            42,
        );
        assert!(signed.bytes().ends_with(&expected));
    }

    #[rstest]
    fn spec376_spot_cancel_scale_encoding_is_exact() {
        let (snapshot, client_state) = runtime();
        let params = DeepXSpotCancelParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            order_id: u64::MAX,
            is_buy: true,
            fast_cancel: true,
        };
        let mut expected = vec![20, 3];
        expected.extend_from_slice(&params.subaccount);
        expected.extend_from_slice(&params.pair);
        expected.extend_from_slice(&params.order_id.to_le_bytes());
        expected.extend_from_slice(&[1, 0, 1]);

        let arguments = params.into_dynamic_arguments();
        assert_eq!(
            encode_call(&snapshot, "SpotMarket", "cancel_order", arguments.clone()),
            expected,
        );
        let signed = sign(
            &snapshot,
            &client_state,
            "SpotMarket",
            "cancel_order",
            arguments,
            64,
        );
        assert!(signed.bytes().ends_with(&expected));
    }

    #[rstest]
    #[case::subaccount(0)]
    #[case::market_id(1)]
    #[case::size(2)]
    #[case::order_type(3)]
    #[case::nonce(4)]
    fn spec376_signing_is_sensitive_to_arguments_and_nonce(#[case] mutation: u8) {
        let (snapshot, client_state) = runtime();
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
        let mut nonce = 42;
        let original = sign(
            &snapshot,
            &client_state,
            "PerpMarket",
            "place_order",
            params.into_dynamic_arguments(),
            nonce,
        );
        match mutation {
            0 => params.subaccount[19] ^= 1,
            1 => params.market_id += 1,
            2 => params.size += 1,
            3 => params.order_type = DeepXPerpOrderType::Market(None),
            4 => nonce += 1,
            _ => unreachable!(),
        }
        let changed = sign(
            &snapshot,
            &client_state,
            "PerpMarket",
            "place_order",
            params.into_dynamic_arguments(),
            nonce,
        );

        assert_ne!(original.bytes(), changed.bytes());
        assert_ne!(original.extrinsic_hash(), changed.extrinsic_hash());
        assert_eq!(original.runtime(), changed.runtime());
        assert_eq!(original.signer(), changed.signer());
    }

    #[rstest]
    fn spec376_metadata_rejects_malformed_arguments() {
        let (snapshot, client_state) = runtime();
        let result = sign_dynamic_pallet_call_with_runtime(
            snapshot.identity(),
            snapshot.interfaces(),
            &client_state,
            &key(),
            "SpotMarket",
            "cancel_order",
            vec![Value::named_composite([(
                "params",
                Value::named_composite([
                    ("subaccount", Value::from_bytes([0x11; 19])),
                    ("pair", Value::from_bytes([0x22; 32])),
                    ("order_id", Value::u128(7)),
                    ("is_buy", Value::bool(false)),
                    (
                        "cancel_reason",
                        Value::unnamed_variant("UserCanceled", Vec::<Value>::new()),
                    ),
                    ("fast_cancel", Value::bool(false)),
                ]),
            )])],
            42,
        );

        assert!(matches!(result, Err(SigningError::Encode(_))));
    }

    #[rstest]
    fn spec376_interface_rejects_unknown_call() {
        let (snapshot, client_state) = runtime();
        let result = sign_dynamic_pallet_call_with_runtime(
            snapshot.identity(),
            snapshot.interfaces(),
            &client_state,
            &key(),
            "UnknownPallet",
            "unknown_call",
            Vec::new(),
            42,
        );

        assert!(matches!(
            result,
            Err(SigningError::RuntimeInterface(
                DeepXRuntimeInterfaceError::PalletUnavailable(pallet),
            )) if pallet == "UnknownPallet"
        ));
    }
}
