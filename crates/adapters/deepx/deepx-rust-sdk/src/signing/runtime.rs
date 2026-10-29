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

//! DeepX AccountId20 runtime configuration and nonce-era extension.

use subxt_core::{
    Config,
    client::ClientState,
    config::{
        DefaultExtrinsicParamsBuilder, ExtrinsicParams, ExtrinsicParamsEncoder,
        TransactionExtension, substrate::BlakeTwo256, transaction_extensions,
    },
    dynamic::Value,
    error::ExtrinsicParamsError,
    tx,
    utils::{AccountId20, H256, Static},
};
use subxt_signer::eth::{Keypair, Signature};
use thiserror::Error;

use super::{
    DeepXNonceEra,
    management::{
        DeepXBuyQuotaParams, DeepXDeleteSubaccountParams, DeepXInitializeSubaccountParams,
        DeepXRemoveDelegateAccountParams, DeepXRenameSubaccountParams,
        DeepXSetDelegateAccountParams, DeepXSetGlobalLeverageParams,
        DeepXSetPerMarketLeverageParams, DeepXSetSpotMarginParams, DeepXUpdateDelegateModeParams,
    },
    pallet::{
        DeepXPerpCancelParams, DeepXPerpCloseParams, DeepXPerpModifyParams, DeepXPerpPlaceParams,
        DeepXPerpProfitAndLossPointParams, DeepXSpotCancelParams, DeepXSpotPlaceParams,
    },
    runtime_interface::{DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError},
};
use crate::credential::DeepXPrivateKey;

/// DeepX runtime types required for AccountId20 Ethereum-compatible signatures.
#[derive(Clone, Copy, Debug)]
pub enum DeepXRuntimeConfig {}

/// DeepX nonce-era transaction extension.
#[doc(hidden)]
#[derive(Debug)]
pub struct CheckNonceEra {
    genesis_hash: H256,
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

/// An offline pallet extrinsic signed against caller-provided runtime state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXEncodedPalletExtrinsic {
    bytes: Vec<u8>,
    extrinsic_hash: [u8; 32],
    signer: [u8; 20],
    nonce: u64,
}

impl DeepXEncodedPalletExtrinsic {
    /// Returns the complete compact-length-prefixed SCALE extrinsic bytes.
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

    /// Returns the encoded nonce.
    #[must_use]
    pub const fn nonce(&self) -> u64 {
        self.nonce
    }

    /// Consumes the result and returns the complete SCALE extrinsic bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Errors produced while signing an offline DeepX pallet call.
#[derive(Debug, Error)]
pub enum DeepXPalletSigningError {
    /// The private scalar was rejected by the pinned DeepX signer implementation.
    #[error("invalid DeepX secp256k1 signing key")]
    InvalidKey,
    /// The pallet call is absent from the runtime interface.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The cancel and place legs of a perpetual modify target different runtime scopes.
    #[error("DeepX perpetual modify legs must target the same subaccount and market")]
    ModifyScopeMismatch,
    /// The runtime accepts at most 32 subaccount name bytes.
    #[error("DeepX subaccount name exceeds the runtime limit of 32 bytes: {length}")]
    SubaccountNameTooLong { length: usize },
    /// The dynamic call or transaction extensions could not be SCALE encoded.
    #[error("unable to encode DeepX pallet extrinsic: {0}")]
    Encode(#[source] Box<subxt_core::Error>),
}

/// A typed DeepX pallet operation encoded against caller-provided runtime state.
#[derive(Clone, Debug)]
pub enum DeepXOfflinePalletCall {
    /// Places a perpetual order.
    PerpPlaceOrder(DeepXPerpPlaceParams),
    /// Places a Spot order.
    SpotPlaceOrder(DeepXSpotPlaceParams),
    /// Cancels a perpetual order.
    PerpCancel(DeepXPerpCancelParams),
    /// Modifies a perpetual order atomically.
    PerpModifyOrder(DeepXPerpModifyParams),
    /// Cancels a Spot order.
    SpotCancel(DeepXSpotCancelParams),
    /// Closes a perpetual position.
    PerpClose(DeepXPerpCloseParams),
    /// Sets perpetual profit and loss points.
    PerpSetProfitAndLossPoint(DeepXPerpProfitAndLossPointParams),
    /// Sets global perpetual leverage.
    SetGlobalLeverage(DeepXSetGlobalLeverageParams),
    /// Sets per-market perpetual leverage.
    SetPerMarketLeverage(DeepXSetPerMarketLeverageParams),
    /// Initializes a subaccount.
    InitializeSubaccount(DeepXInitializeSubaccountParams),
    /// Deletes a subaccount.
    DeleteSubaccount(DeepXDeleteSubaccountParams),
    /// Renames a subaccount.
    RenameSubaccount(DeepXRenameSubaccountParams),
    /// Changes Spot margin mode.
    SetSpotMargin(DeepXSetSpotMarginParams),
    /// Adds or renews a delegate account.
    SetDelegateAccount(DeepXSetDelegateAccountParams),
    /// Updates a delegate account mode.
    UpdateDelegateMode(DeepXUpdateDelegateModeParams),
    /// Removes a delegate account.
    RemoveDelegateAccount(DeepXRemoveDelegateAccountParams),
    /// Buys Lending quota.
    BuyQuota(DeepXBuyQuotaParams),
    /// Signs a Subaccount no-op.
    NoOp,
}

/// Signs one typed DeepX pallet operation without submitting it.
///
/// The caller validates runtime identity and metadata, allocates the nonce, and verifies finality.
///
/// # Errors
///
/// Returns an error for invalid operation scope or name, an unavailable call, invalid key, or
/// failed SCALE encoding.
pub fn sign_pallet_call(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    client_state: &ClientState<DeepXRuntimeConfig>,
    key: &DeepXPrivateKey,
    operation: DeepXOfflinePalletCall,
    nonce: u64,
) -> Result<DeepXEncodedPalletExtrinsic, DeepXPalletSigningError> {
    let (pallet, call, arguments) = match operation {
        DeepXOfflinePalletCall::PerpPlaceOrder(params) => {
            ("PerpMarket", "place_order", params.into_dynamic_arguments())
        }
        DeepXOfflinePalletCall::SpotPlaceOrder(params) => {
            ("SpotMarket", "place_order", params.into_dynamic_arguments())
        }
        DeepXOfflinePalletCall::PerpCancel(params) => (
            "PerpMarket",
            "cancel_order",
            params.into_dynamic_arguments(),
        ),
        DeepXOfflinePalletCall::PerpModifyOrder(params) => {
            if params.cancel.subaccount != params.place.subaccount
                || params.cancel.market_id != params.place.market_id
            {
                return Err(DeepXPalletSigningError::ModifyScopeMismatch);
            }
            (
                "Subaccount",
                "modify_orders",
                params.into_dynamic_arguments(),
            )
        }
        DeepXOfflinePalletCall::SpotCancel(params) => (
            "SpotMarket",
            "cancel_order",
            params.into_dynamic_arguments(),
        ),
        DeepXOfflinePalletCall::PerpClose(params) => (
            "PerpMarket",
            "close_position",
            params.into_dynamic_arguments(),
        ),
        DeepXOfflinePalletCall::PerpSetProfitAndLossPoint(params) => (
            "PerpMarket",
            "set_profit_and_loss_point",
            params.into_dynamic_arguments(),
        ),
        DeepXOfflinePalletCall::SetGlobalLeverage(params) => (
            "PerpMarket",
            "set_global_leverage",
            params.into_dynamic_arguments(),
        ),
        DeepXOfflinePalletCall::SetPerMarketLeverage(params) => (
            "PerpMarket",
            "set_per_market_leverage",
            params.into_dynamic_arguments(),
        ),
        DeepXOfflinePalletCall::InitializeSubaccount(params) => {
            if params.name.len() > 32 {
                return Err(DeepXPalletSigningError::SubaccountNameTooLong {
                    length: params.name.len(),
                });
            }
            (
                "Subaccount",
                "initialize_subaccount",
                params.into_dynamic_arguments(),
            )
        }
        DeepXOfflinePalletCall::DeleteSubaccount(params) => (
            "Subaccount",
            "delete_subaccount",
            params.into_dynamic_arguments(),
        ),
        DeepXOfflinePalletCall::RenameSubaccount(params) => {
            if params.new_name.len() > 32 {
                return Err(DeepXPalletSigningError::SubaccountNameTooLong {
                    length: params.new_name.len(),
                });
            }
            (
                "Subaccount",
                "rename_subaccount",
                params.into_dynamic_arguments(),
            )
        }
        DeepXOfflinePalletCall::SetSpotMargin(params) => (
            "Subaccount",
            "set_spot_margin",
            params.into_dynamic_arguments(),
        ),
        DeepXOfflinePalletCall::SetDelegateAccount(params) => (
            "Subaccount",
            "set_delegate_account",
            params.into_dynamic_arguments(),
        ),
        DeepXOfflinePalletCall::UpdateDelegateMode(params) => (
            "Subaccount",
            "update_delegate_mode",
            params.into_dynamic_arguments(),
        ),
        DeepXOfflinePalletCall::RemoveDelegateAccount(params) => (
            "Subaccount",
            "remove_delegate_account",
            params.into_dynamic_arguments(),
        ),
        DeepXOfflinePalletCall::BuyQuota(params) => {
            ("Lending", "buy_quota", params.into_dynamic_arguments())
        }
        DeepXOfflinePalletCall::NoOp => ("Subaccount", "no_op", Vec::new()),
    };
    sign_dynamic_pallet_call(
        interfaces,
        client_state,
        key,
        pallet,
        call,
        arguments,
        nonce,
    )
}

impl From<subxt_core::Error> for DeepXPalletSigningError {
    fn from(value: subxt_core::Error) -> Self {
        Self::Encode(Box::new(value))
    }
}

/// Derives the Ethereum-compatible DeepX account identity for a signing key.
///
/// # Errors
///
/// Returns an error if the pinned signer implementation rejects the private scalar.
pub fn derive_signer_account_id(
    key: &DeepXPrivateKey,
) -> Result<[u8; 20], DeepXPalletSigningError> {
    let signer = Keypair::from_secret_key(*key.as_bytes())
        .map_err(|_| DeepXPalletSigningError::InvalidKey)?;
    Ok(signer.public_key().to_account_id().0)
}

/// Signs a metadata-driven DeepX pallet call against caller-provided runtime state.
///
/// This offline operation performs no runtime approval, nonce reservation, submission, or
/// finality verification. The caller must validate the runtime identity and metadata before
/// supplying the client state and interface catalog.
///
/// # Errors
///
/// Returns an error for an unavailable call, invalid key, or failed SCALE encoding.
pub fn sign_dynamic_pallet_call(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    client_state: &ClientState<DeepXRuntimeConfig>,
    key: &DeepXPrivateKey,
    pallet: &str,
    call: &str,
    arguments: Vec<Value>,
    nonce: u64,
) -> Result<DeepXEncodedPalletExtrinsic, DeepXPalletSigningError> {
    interfaces.call(pallet, call)?;

    let signer = Keypair::from_secret_key(*key.as_bytes())
        .map_err(|_| DeepXPalletSigningError::InvalidKey)?;
    let account_id = signer.public_key().to_account_id().0;
    let payload = subxt_core::dynamic::tx(pallet, call, arguments);
    let transaction =
        tx::create_v4_signed(&payload, client_state, immortal_params(nonce))?.sign(&signer);
    let hash = transaction.hash_with(BlakeTwo256);

    Ok(DeepXEncodedPalletExtrinsic {
        bytes: transaction.into_encoded(),
        extrinsic_hash: hash.0,
        signer: account_id,
        nonce,
    })
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

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde::Deserialize;
    use subxt_core::{client::RuntimeVersion, config::Hasher, metadata};

    use super::*;
    use crate::{
        credential::DeepXKeyScheme,
        signing::{
            DeepXPerpOrderType, DeepXPerpPlaceParams, DeepXPostOnlyParam, DeepXSpotCancelParams,
            DeepXSpotOrderType, DeepXSpotPlaceParams, DeepXTimeInForce,
        },
    };

    const SPEC376_METADATA: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test_data/runtime/testnet/",
        "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
    ));

    #[derive(Deserialize)]
    struct RpcResponse {
        result: String,
    }

    #[rstest]
    fn nonce_era_extension_encodes_captured_immortal_extra_and_genesis() {
        let extension = CheckNonceEra {
            genesis_hash: H256([0xab; 32]),
        };
        let mut extra = Vec::new();
        let mut implicit = Vec::new();
        extension.encode_value_to(&mut extra);
        extension.encode_implicit_to(&mut implicit);

        assert_eq!(extra, [0]);
        assert_eq!(implicit, [0xab; 32]);
    }

    #[rstest]
    fn signs_captured_spec376_no_op_extrinsic_exactly() {
        let response: RpcResponse = serde_json::from_str(SPEC376_METADATA).unwrap();
        let metadata_bytes = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let metadata = metadata::decode_from(&metadata_bytes).unwrap();
        let interfaces = DeepXRuntimeInterfaceCatalog::from_metadata(&metadata).unwrap();
        let genesis =
            hex::decode("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap();
        let client_state = ClientState::<DeepXRuntimeConfig> {
            metadata,
            genesis_hash: H256::from_slice(&genesis),
            runtime_version: RuntimeVersion {
                spec_version: 376,
                transaction_version: 1,
            },
        };
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();

        let signed = sign_dynamic_pallet_call(
            &interfaces,
            &client_state,
            &key,
            "Subaccount",
            "no_op",
            Vec::new(),
            1_725_000_000_124,
        )
        .unwrap();

        assert_eq!(
            hex::encode(signed.bytes()),
            "850184fcad0b19bb29d4674531d6f115237e16afce377ccd76f6b9583b6dfd829576bc6888f00fa985b12eb69488d9215ad76133b8792113e341e9f2dc4a0e073af90495f8926c9d4c9e817f29ee384e83fb3dd18d78f600000b7c2203a2910100131c",
        );
        assert_eq!(signed.signer(), derive_signer_account_id(&key).unwrap());
        assert_eq!(signed.extrinsic_hash(), BlakeTwo256.hash(signed.bytes()).0);

        let typed = sign_pallet_call(
            &interfaces,
            &client_state,
            &key,
            DeepXOfflinePalletCall::NoOp,
            1_725_000_000_124,
        )
        .unwrap();
        assert_eq!(typed, signed);

        let invalid_modify = DeepXPerpModifyParams {
            cancel: DeepXPerpCancelParams {
                subaccount: [0x11; 20],
                order_id: 42,
                market_id: 3,
                fast_cancel: false,
            },
            place: DeepXPerpPlaceParams {
                subaccount: [0x22; 20],
                market_id: 3,
                is_long: true,
                size: 1,
                price: 2,
                order_type: DeepXPerpOrderType::Limit(DeepXTimeInForce::Gtc),
                take_profit: None,
                stop_loss: None,
                reduce_only: false,
                post_only: DeepXPostOnlyParam::None,
            },
        };
        assert!(matches!(
            sign_pallet_call(
                &interfaces,
                &client_state,
                &key,
                DeepXOfflinePalletCall::PerpModifyOrder(invalid_modify),
                1,
            ),
            Err(DeepXPalletSigningError::ModifyScopeMismatch)
        ));
        assert!(matches!(
            sign_pallet_call(
                &interfaces,
                &client_state,
                &key,
                DeepXOfflinePalletCall::InitializeSubaccount(DeepXInitializeSubaccountParams {
                    name: vec![b'x'; 33],
                }),
                1,
            ),
            Err(DeepXPalletSigningError::SubaccountNameTooLong { length: 33 })
        ));
    }

    #[rstest]
    fn spec376_order_call_data_encodes_exactly() {
        let response: RpcResponse = serde_json::from_str(SPEC376_METADATA).unwrap();
        let metadata_bytes = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let metadata = metadata::decode_from(&metadata_bytes).unwrap();

        let perp_place = DeepXPerpPlaceParams {
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
        let mut perp_expected = vec![22, 2];
        perp_expected.extend_from_slice(&perp_place.subaccount);
        perp_expected.extend_from_slice(&perp_place.market_id.to_le_bytes());
        perp_expected.push(1);
        perp_expected.extend_from_slice(&perp_place.size.to_le_bytes());
        perp_expected.extend_from_slice(&perp_place.price.to_le_bytes());
        perp_expected.extend_from_slice(&[0, 1, 1]);
        perp_expected.extend_from_slice(&789_u128.to_le_bytes());
        perp_expected.extend_from_slice(&[0, 1, 1]);

        let spot_place = DeepXSpotPlaceParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            is_buy: true,
            quote_amount: [0x33; 32],
            base_amount: [0x44; 32],
            order_type: DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc),
            post_only: DeepXPostOnlyParam::None,
            reduce_only: false,
        };
        let mut spot_expected = vec![20, 1];
        spot_expected.extend_from_slice(&spot_place.subaccount);
        spot_expected.extend_from_slice(&spot_place.pair);
        spot_expected.push(1);
        spot_expected.extend_from_slice(&spot_place.quote_amount);
        spot_expected.extend_from_slice(&spot_place.base_amount);
        spot_expected.extend_from_slice(&[0, 0, 0, 0]);

        let spot_cancel = DeepXSpotCancelParams {
            subaccount: [0x11; 20],
            pair: [0x22; 32],
            order_id: u64::MAX,
            is_buy: true,
            fast_cancel: true,
        };
        let mut cancel_expected = vec![20, 3];
        cancel_expected.extend_from_slice(&spot_cancel.subaccount);
        cancel_expected.extend_from_slice(&spot_cancel.pair);
        cancel_expected.extend_from_slice(&spot_cancel.order_id.to_le_bytes());
        cancel_expected.extend_from_slice(&[1, 0, 1]);

        let cases = [
            (
                "PerpMarket",
                "place_order",
                perp_place.into_dynamic_arguments(),
                perp_expected,
            ),
            (
                "SpotMarket",
                "place_order",
                spot_place.into_dynamic_arguments(),
                spot_expected,
            ),
            (
                "SpotMarket",
                "cancel_order",
                spot_cancel.into_dynamic_arguments(),
                cancel_expected,
            ),
        ];
        for (pallet, call, arguments, expected) in cases {
            let payload = subxt_core::dynamic::tx(pallet, call, arguments);
            let encoded = tx::payload::Payload::encode_call_data(&payload, &metadata).unwrap();
            assert_eq!(encoded, expected, "{pallet}.{call}");
        }
    }

    fn spec376_signing_context() -> (
        DeepXRuntimeInterfaceCatalog,
        ClientState<DeepXRuntimeConfig>,
        DeepXPrivateKey,
    ) {
        let response: RpcResponse = serde_json::from_str(SPEC376_METADATA).unwrap();
        let metadata_bytes = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let metadata = metadata::decode_from(&metadata_bytes).unwrap();
        let interfaces = DeepXRuntimeInterfaceCatalog::from_metadata(&metadata).unwrap();
        let genesis =
            hex::decode("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap();
        let client_state = ClientState::<DeepXRuntimeConfig> {
            metadata,
            genesis_hash: H256::from_slice(&genesis),
            runtime_version: RuntimeVersion {
                spec_version: 376,
                transaction_version: 1,
            },
        };
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        (interfaces, client_state, key)
    }

    #[rstest]
    fn spec376_signed_extrinsic_vectors_match_captured_bytes() {
        let (interfaces, state, key) = spec376_signing_context();
        let cases = [
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
            let signed =
                sign_dynamic_pallet_call(&interfaces, &state, &key, pallet, call, arguments, nonce)
                    .unwrap();
            assert_eq!(
                signed.bytes(),
                hex::decode(expected).unwrap(),
                "{pallet}.{call}"
            );
            assert_eq!(signed.extrinsic_hash(), BlakeTwo256.hash(signed.bytes()).0);
        }
    }

    #[rstest]
    #[case::subaccount(0)]
    #[case::market_id(1)]
    #[case::size(2)]
    #[case::order_type(3)]
    #[case::nonce(4)]
    fn spec376_signature_binds_order_arguments_and_nonce(#[case] mutation: u8) {
        let (interfaces, state, key) = spec376_signing_context();
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
        let original = sign_pallet_call(
            &interfaces,
            &state,
            &key,
            DeepXOfflinePalletCall::PerpPlaceOrder(params),
            nonce,
        )
        .unwrap();
        match mutation {
            0 => params.subaccount[19] ^= 1,
            1 => params.market_id += 1,
            2 => params.size += 1,
            3 => params.order_type = DeepXPerpOrderType::Market(None),
            4 => nonce += 1,
            _ => unreachable!(),
        }
        let changed = sign_pallet_call(
            &interfaces,
            &state,
            &key,
            DeepXOfflinePalletCall::PerpPlaceOrder(params),
            nonce,
        )
        .unwrap();
        assert_ne!(original.bytes(), changed.bytes());
        assert_ne!(original.extrinsic_hash(), changed.extrinsic_hash());
        assert_eq!(original.signer(), changed.signer());
    }

    #[rstest]
    fn spec376_rejects_malformed_arguments_and_unknown_calls() {
        let (interfaces, state, key) = spec376_signing_context();
        let malformed = sign_dynamic_pallet_call(
            &interfaces,
            &state,
            &key,
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
        assert!(matches!(malformed, Err(DeepXPalletSigningError::Encode(_))));

        let unknown = sign_dynamic_pallet_call(
            &interfaces,
            &state,
            &key,
            "UnknownPallet",
            "unknown_call",
            Vec::new(),
            42,
        );
        assert!(matches!(
            unknown,
            Err(DeepXPalletSigningError::RuntimeInterface(
                DeepXRuntimeInterfaceError::PalletUnavailable(pallet),
            )) if pallet == "UnknownPallet"
        ));
    }
}
