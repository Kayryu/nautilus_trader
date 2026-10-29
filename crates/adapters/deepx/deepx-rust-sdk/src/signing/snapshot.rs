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

//! Validated DeepX testnet runtime identity and metadata.

use aws_lc_rs::digest::{SHA256, digest};
use scale_info::TypeDef;
use subxt_core::{metadata, metadata::Metadata};
use thiserror::Error;

use super::runtime_interface::{DeepXRuntimeInterfaceCatalog, DeepXRuntimeInterfaceError};
use crate::enums::DeepXEnvironment;

const TESTNET_SPEC_376_GENESIS_HASH: &str =
    "87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7";

/// Runtime identity validated against the DeepX testnet genesis and observed metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovedRuntimeIdentity {
    /// DeepX deployment associated with the observed runtime.
    pub environment: DeepXEnvironment,
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

/// Errors while validating an immutable DeepX runtime snapshot.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// The pinned identity constant could not be decoded.
    #[error("invalid built-in DeepX runtime identity")]
    InvalidApprovedIdentity,
    /// Runtime snapshots are currently approved only for DeepX testnet.
    #[error("unsupported DeepX runtime deployment: {0}")]
    UnsupportedDeployment(String),
    /// The SCALE metadata cannot be decoded by the pinned DeepX Subxt fork.
    #[error("invalid DeepX runtime metadata: {0}")]
    InvalidMetadata(#[from] parity_scale_codec::Error),
    /// The built-in approved metadata fixture cannot be decoded.
    #[error("invalid built-in DeepX runtime metadata")]
    InvalidBuiltInMetadata,
    /// The observed genesis is not testnet or a runtime version is zero.
    #[error("invalid DeepX genesis or runtime version")]
    RuntimeIdentityMismatch,
    /// An unknown non-empty transaction extension would make signing ambiguous.
    #[error("unsupported non-empty DeepX transaction extension: {0}")]
    UnsupportedTransactionExtension(String),
    /// The approved metadata contains an inconsistent pallet, call, event, or error identity.
    #[error(transparent)]
    InvalidRuntimeInterface(#[from] DeepXRuntimeInterfaceError),
}

/// Decoded metadata and interfaces bound to one verified runtime identity.
#[derive(Clone, Debug)]
pub struct DeepXValidatedRuntime {
    identity: ApprovedRuntimeIdentity,
    interfaces: DeepXRuntimeInterfaceCatalog,
    metadata: Metadata,
}

impl DeepXValidatedRuntime {
    /// Returns the validated runtime identity.
    #[must_use]
    pub const fn identity(&self) -> &ApprovedRuntimeIdentity {
        &self.identity
    }

    /// Returns the pallet, call, event, and error catalog.
    #[must_use]
    pub const fn interfaces(&self) -> &DeepXRuntimeInterfaceCatalog {
        &self.interfaces
    }

    /// Returns the decoded runtime metadata.
    #[must_use]
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Consumes the validated runtime and returns its owned components.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ApprovedRuntimeIdentity,
        DeepXRuntimeInterfaceCatalog,
        Metadata,
    ) {
        (self.identity, self.interfaces, self.metadata)
    }
}

/// Validates observed DeepX testnet genesis, metadata, versions, and transaction extensions.
///
/// # Errors
///
/// Returns an error for another deployment or genesis, invalid metadata or versions, unsupported
/// signing extensions, or an inconsistent runtime interface.
pub fn validate_testnet_runtime(
    environment: &DeepXEnvironment,
    observed_genesis_hash: [u8; 32],
    observed_spec_version: u32,
    observed_transaction_version: u32,
    metadata_bytes: &[u8],
) -> Result<DeepXValidatedRuntime, SnapshotError> {
    if !environment.is_testnet() {
        return Err(SnapshotError::UnsupportedDeployment(
            environment.to_string(),
        ));
    }
    let genesis = decode_32(TESTNET_SPEC_376_GENESIS_HASH)?;
    if observed_genesis_hash != genesis
        || observed_spec_version == 0
        || observed_transaction_version == 0
    {
        return Err(SnapshotError::RuntimeIdentityMismatch);
    }

    let metadata = metadata::decode_from(metadata_bytes)?;
    validate_unknown_extensions(&metadata)?;
    let signed_extensions = metadata
        .extrinsic()
        .transaction_extensions_to_use_for_encoding()
        .map(|extension| extension.identifier().to_string())
        .collect();
    let mut metadata_sha256 = [0; 32];
    metadata_sha256.copy_from_slice(digest(&SHA256, metadata_bytes).as_ref());
    let interfaces = DeepXRuntimeInterfaceCatalog::from_metadata(&metadata)?;
    let identity = ApprovedRuntimeIdentity {
        environment: environment.clone(),
        genesis_hash: observed_genesis_hash,
        metadata_sha256,
        spec_version: observed_spec_version,
        transaction_version: observed_transaction_version,
        signed_extensions,
    };

    Ok(DeepXValidatedRuntime {
        identity,
        interfaces,
        metadata,
    })
}

fn validate_unknown_extensions(metadata: &Metadata) -> Result<(), SnapshotError> {
    const SUPPORTED: &[&str] = &[
        "CheckSpecVersion",
        "CheckTxVersion",
        "CheckNonce",
        "CheckGenesis",
        "CheckMortality",
        "CheckNonceEra",
        "ChargeAssetTxPayment",
        "ChargeTransactionPayment",
        "CheckMetadataHash",
    ];

    for extension in metadata
        .extrinsic()
        .transaction_extensions_to_use_for_encoding()
    {
        if !SUPPORTED.contains(&extension.identifier())
            && (!is_empty_type(extension.extra_ty(), metadata)
                || !is_empty_type(extension.additional_ty(), metadata))
        {
            return Err(SnapshotError::UnsupportedTransactionExtension(
                extension.identifier().to_string(),
            ));
        }
    }
    Ok(())
}

fn is_empty_type(type_id: u32, metadata: &Metadata) -> bool {
    let Some(ty) = metadata.types().resolve(type_id) else {
        return false;
    };
    match &ty.type_def {
        TypeDef::Composite(value) => value
            .fields
            .iter()
            .all(|field| is_empty_type(field.ty.id, metadata)),
        TypeDef::Array(value) => value.len == 0 || is_empty_type(value.type_param.id, metadata),
        TypeDef::Tuple(value) => value
            .fields
            .iter()
            .all(|field| is_empty_type(field.id, metadata)),
        TypeDef::BitSequence(_)
        | TypeDef::Variant(_)
        | TypeDef::Sequence(_)
        | TypeDef::Compact(_)
        | TypeDef::Primitive(_) => false,
    }
}

fn decode_32(value: &str) -> Result<[u8; 32], SnapshotError> {
    let mut decoded = [0; 32];
    hex::decode_to_slice(value, &mut decoded)
        .map_err(|_| SnapshotError::InvalidApprovedIdentity)?;
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use frame_metadata::{
        META_RESERVED, RuntimeMetadata, RuntimeMetadataPrefixed,
        v14::{StorageEntryModifier, StorageEntryType, StorageHasher},
    };
    use parity_scale_codec::{Decode, Encode};
    use rstest::rstest;
    use scale_info::{TypeDef, TypeDefPrimitive, Variant};

    use super::*;
    use crate::signing::runtime_interface::DeepXRuntimeInterfaceError;

    const METADATA_RESPONSE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test_data/runtime/testnet/",
        "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
    ));
    const METADATA_SHA256: &str =
        "57f1bedf1e04522176a2b422af9249d5c990693fb92d11327870d8ef751a8447";

    fn fixture_metadata() -> Vec<u8> {
        let response: serde_json::Value = serde_json::from_str(METADATA_RESPONSE).unwrap();
        let encoded = response["result"].as_str().unwrap();
        hex::decode(encoded.trim_start_matches("0x")).unwrap()
    }

    #[rstest]
    fn validates_captured_testnet_runtime() {
        let metadata = fixture_metadata();
        let genesis = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();

        let validated =
            validate_testnet_runtime(&DeepXEnvironment::Testnet, genesis, 376, 1, &metadata)
                .unwrap();

        assert_eq!(validated.identity().genesis_hash, genesis);
        assert_eq!(
            hex::encode(validated.identity().metadata_sha256),
            METADATA_SHA256,
        );
        assert_eq!(validated.identity().signed_extensions[4], "CheckNonceEra");
        assert!(validated.interfaces().pallet("PerpMarket").is_ok());
    }

    #[rstest]
    #[case(365)]
    #[case(376)]
    #[case(377)]
    #[case(u32::MAX)]
    fn accepts_observed_runtime_spec_versions(#[case] spec_version: u32) {
        let metadata = fixture_metadata();
        let genesis = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();
        let validated = validate_testnet_runtime(
            &DeepXEnvironment::Testnet,
            genesis,
            spec_version,
            1,
            &metadata,
        )
        .unwrap();

        assert_eq!(validated.identity().spec_version, spec_version);
        assert_eq!(validated.identity().signed_extensions[4], "CheckNonceEra");
        assert_eq!(
            hex::encode(validated.identity().metadata_sha256),
            METADATA_SHA256,
        );
    }

    #[rstest]
    fn accepts_changed_metadata_with_the_same_runtime_interface() {
        let fixture = fixture_metadata();
        let mut input = fixture.as_slice();
        let mut decoded = RuntimeMetadataPrefixed::decode(&mut input).unwrap();
        let RuntimeMetadata::V14(metadata) = &mut decoded.1 else {
            panic!("expected V14 fixture metadata");
        };
        metadata
            .pallets
            .iter_mut()
            .find_map(|pallet| pallet.constants.first_mut())
            .unwrap()
            .docs
            .push("Updated runtime documentation".to_string());
        let changed = decoded.encode();
        let genesis = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();

        let original =
            validate_testnet_runtime(&DeepXEnvironment::Testnet, genesis, 376, 1, &fixture)
                .unwrap();
        let updated =
            validate_testnet_runtime(&DeepXEnvironment::Testnet, genesis, 377, 2, &changed)
                .unwrap();

        assert_ne!(
            updated.identity().metadata_sha256,
            original.identity().metadata_sha256
        );
        assert_eq!(updated.identity().spec_version, 377);
        assert_eq!(updated.identity().transaction_version, 2);
        assert_eq!(updated.interfaces(), original.interfaces());
    }

    #[rstest]
    fn rejects_wrong_genesis_and_invalid_metadata() {
        let metadata = fixture_metadata();
        let genesis = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();

        assert!(matches!(
            validate_testnet_runtime(&DeepXEnvironment::Testnet, [0; 32], 376, 1, &metadata),
            Err(SnapshotError::RuntimeIdentityMismatch),
        ));
        assert!(matches!(
            validate_testnet_runtime(&DeepXEnvironment::Testnet, genesis, 376, 1, &metadata[..16]),
            Err(SnapshotError::InvalidMetadata(_)),
        ));
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
        let metadata = fixture_metadata();
        let genesis = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();
        let runtime =
            validate_testnet_runtime(&DeepXEnvironment::Testnet, genesis, 376, 1, &metadata)
                .unwrap();
        let pallet = runtime.metadata().pallet_by_name(pallet_name).unwrap();
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
        runtime.interfaces().call(pallet_name, call_name).unwrap();
    }

    #[rstest]
    fn runtime_interface_lookups_fail_closed() {
        let metadata = fixture_metadata();
        let genesis = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();
        let runtime =
            validate_testnet_runtime(&DeepXEnvironment::Testnet, genesis, 376, 1, &metadata)
                .unwrap();

        assert_eq!(
            runtime.interfaces().pallet("UnknownPallet"),
            Err(DeepXRuntimeInterfaceError::PalletUnavailable(
                "UnknownPallet".to_string(),
            )),
        );
        assert_eq!(
            runtime.interfaces().call("System", "unknown_call"),
            Err(DeepXRuntimeInterfaceError::CallUnavailable(
                "System".to_string(),
                "unknown_call".to_string(),
            )),
        );
        assert_eq!(
            runtime.interfaces().event("System", "UnknownEvent"),
            Err(DeepXRuntimeInterfaceError::EventUnavailable(
                "System".to_string(),
                "UnknownEvent".to_string(),
            )),
        );
    }

    #[rstest]
    fn spec_376_perp_position_storage_metadata_layout_is_exact() {
        let bytes = fixture_metadata();
        let mut input = bytes.as_slice();
        let RuntimeMetadataPrefixed(prefix, RuntimeMetadata::V14(metadata)) =
            RuntimeMetadataPrefixed::decode(&mut input).unwrap()
        else {
            panic!("Expected V14 fixture metadata");
        };
        assert_eq!(prefix, META_RESERVED);
        assert!(input.is_empty());
        let perp_market = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.name == "PerpMarket")
            .unwrap();
        let perp_storage = perp_market.storage.as_ref().unwrap();
        assert_eq!(perp_storage.prefix, "PerpMarket");
        let positions = perp_storage
            .entries
            .iter()
            .find(|entry| entry.name == "UserPerpPositions")
            .unwrap();
        assert_eq!(positions.modifier, StorageEntryModifier::Default);
        let StorageEntryType::Map {
            hashers,
            key,
            value,
        } = &positions.ty
        else {
            panic!("UserPerpPositions must be a storage map");
        };
        assert!(matches!(hashers.as_slice(), [StorageHasher::Identity]));
        assert_fixed_u8_array(&metadata.types, key.id, 20, Some("H160"));
        assert_eq!(positions.default, [0]);

        let position_sequence = metadata.types.resolve(value.id).unwrap();
        let TypeDef::Sequence(position_sequence) = &position_sequence.type_def else {
            panic!("UserPerpPositions value must be Vec<PerpPosition>");
        };
        let position = metadata
            .types
            .resolve(position_sequence.type_param.id)
            .unwrap();
        assert_eq!(
            position.path.segments.last().map(String::as_str),
            Some("PerpPosition")
        );
        let TypeDef::Composite(position) = &position.type_def else {
            panic!("PerpPosition must be a composite type");
        };
        let expected_fields = [
            ("market_id", TypeDefPrimitive::U16),
            ("is_long", TypeDefPrimitive::Bool),
            ("base_asset_amount", TypeDefPrimitive::U128),
            ("entry_price", TypeDefPrimitive::U128),
            ("leverage", TypeDefPrimitive::U64),
            ("last_funding_rate", TypeDefPrimitive::I128),
            ("version", TypeDefPrimitive::U64),
            ("realized_pnl", TypeDefPrimitive::I128),
            ("funding_payment", TypeDefPrimitive::I128),
        ];
        assert_eq!(position.fields.len(), 13);
        for (field, (expected_name, expected_type)) in position.fields.iter().zip(expected_fields) {
            assert_eq!(field.name.as_deref(), Some(expected_name));
            assert_primitive(&metadata.types, field.ty.id, expected_type);
        }
        assert_eq!(position.fields[9].name.as_deref(), Some("owner"));
        assert_fixed_u8_array(&metadata.types, position.fields[9].ty.id, 20, Some("H160"));
        assert_eq!(position.fields[10].name.as_deref(), Some("take_profit"));
        assert_option_primitive(
            &metadata.types,
            position.fields[10].ty.id,
            TypeDefPrimitive::U128,
        );
        assert_eq!(position.fields[11].name.as_deref(), Some("stop_loss"));
        assert_option_primitive(
            &metadata.types,
            position.fields[11].ty.id,
            TypeDefPrimitive::U128,
        );
        assert_eq!(
            position.fields[12].name.as_deref(),
            Some("last_settle_price")
        );
        assert_primitive(
            &metadata.types,
            position.fields[12].ty.id,
            TypeDefPrimitive::U128,
        );

        let subaccount = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.name == "Subaccount")
            .unwrap();
        let subaccount_storage = subaccount.storage.as_ref().unwrap();
        assert_eq!(subaccount_storage.prefix, "Subaccount");
        let maximum = subaccount_storage
            .entries
            .iter()
            .find(|entry| entry.name == "MaxPerpPositions")
            .unwrap();
        assert_eq!(maximum.modifier, StorageEntryModifier::Default);
        let StorageEntryType::Plain(maximum_ty) = &maximum.ty else {
            panic!("MaxPerpPositions must be plain storage");
        };
        assert_primitive(&metadata.types, maximum_ty.id, TypeDefPrimitive::U32);
        assert_eq!(maximum.default, 8_u32.encode());
    }

    #[rstest]
    fn spec_376_perp_order_storage_metadata_layout_is_exact() {
        let RuntimeMetadataPrefixed(_, RuntimeMetadata::V14(metadata)) =
            RuntimeMetadataPrefixed::decode(&mut fixture_metadata().as_slice()).unwrap()
        else {
            panic!("Expected V14 fixture metadata");
        };
        let storage = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.name == "PerpMarket")
            .unwrap()
            .storage
            .as_ref()
            .unwrap();

        let order_info = storage
            .entries
            .iter()
            .find(|entry| entry.name == "OrderInfoForMarket")
            .unwrap();
        assert_eq!(order_info.modifier, StorageEntryModifier::Optional);
        assert_eq!(order_info.default, [0]);
        let StorageEntryType::Map {
            hashers,
            key,
            value,
        } = &order_info.ty
        else {
            panic!("OrderInfoForMarket must be a storage map");
        };
        assert!(matches!(
            hashers.as_slice(),
            [StorageHasher::Identity, StorageHasher::Identity]
        ));
        let order_key = metadata.types.resolve(key.id).unwrap();
        let TypeDef::Tuple(order_key) = &order_key.type_def else {
            panic!("OrderInfoForMarket key must be a tuple");
        };
        assert_eq!(order_key.fields.len(), 2);
        assert_fixed_u8_array(&metadata.types, order_key.fields[0].id, 20, Some("H160"));
        assert_primitive(
            &metadata.types,
            order_key.fields[1].id,
            TypeDefPrimitive::U16,
        );
        let orders = metadata.types.resolve(value.id).unwrap();
        assert_eq!(
            orders.path.segments.last().map(String::as_str),
            Some("BTreeMap")
        );
        assert_eq!(orders.type_params.len(), 2);
        assert_primitive(
            &metadata.types,
            orders.type_params[0].ty.as_ref().unwrap().id,
            TypeDefPrimitive::U64,
        );
        let order = metadata
            .types
            .resolve(orders.type_params[1].ty.as_ref().unwrap().id)
            .unwrap();
        assert_eq!(
            order.path.segments.last().map(String::as_str),
            Some("PerpOrder")
        );
        let TypeDef::Composite(order) = &order.type_def else {
            panic!("PerpOrder must be a composite type");
        };
        assert_eq!(order.fields.len(), 16);
        let primitive_fields = [
            (0, "order_id", TypeDefPrimitive::U64),
            (2, "market_id", TypeDefPrimitive::U16),
            (3, "is_long", TypeDefPrimitive::Bool),
            (4, "size", TypeDefPrimitive::U128),
            (5, "price", TypeDefPrimitive::U128),
            (7, "create_time", TypeDefPrimitive::U64),
            (8, "leverage", TypeDefPrimitive::U64),
            (10, "size_filled", TypeDefPrimitive::U128),
            (11, "size_remain", TypeDefPrimitive::U128),
            (14, "reduce_only", TypeDefPrimitive::Bool),
        ];
        for (index, name, primitive) in primitive_fields {
            assert_eq!(order.fields[index].name.as_deref(), Some(name));
            assert_primitive(&metadata.types, order.fields[index].ty.id, primitive);
        }
        assert_eq!(order.fields[1].name.as_deref(), Some("owner"));
        assert_fixed_u8_array(&metadata.types, order.fields[1].ty.id, 20, Some("H160"));
        assert_eq!(order.fields[6].name.as_deref(), Some("order_type"));
        assert_order_type(&metadata.types, order.fields[6].ty.id);
        assert_eq!(order.fields[9].name.as_deref(), Some("status"));
        assert_unit_variants(
            &metadata.types,
            order.fields[9].ty.id,
            &[
                "Open",
                "PartiallyFilled",
                "Filled",
                "Canceled",
                "Rejected",
                "Expired",
            ],
        );
        for (index, name) in [(12, "take_profit"), (13, "stop_loss")] {
            assert_eq!(order.fields[index].name.as_deref(), Some(name));
            assert_option_primitive(
                &metadata.types,
                order.fields[index].ty.id,
                TypeDefPrimitive::U128,
            );
        }
        assert_eq!(order.fields[15].name.as_deref(), Some("post_only"));
        assert_unit_variants(
            &metadata.types,
            order.fields[15].ty.id,
            &["None", "MustPostOnly", "Adaptive"],
        );
    }

    fn assert_order_type(types: &scale_info::PortableRegistry, type_id: u32) {
        let ty = types.resolve(type_id).unwrap();
        assert_eq!(
            ty.path.segments.last().map(String::as_str),
            Some("OrderType")
        );
        let TypeDef::Variant(order_type) = &ty.type_def else {
            panic!("OrderType must be a variant type");
        };
        assert_eq!(
            order_type
                .variants
                .iter()
                .map(|variant| variant.name.as_str())
                .collect::<Vec<_>>(),
            ["Limit", "Market", "Stop"],
        );
        assert_eq!(order_type.variants[0].fields.len(), 1);
        assert_unit_variants(
            types,
            order_type.variants[0].fields[0].ty.id,
            &["GTC", "IOC", "FOK"],
        );
        assert_eq!(order_type.variants[1].fields.len(), 1);
        assert_option_primitive(
            types,
            order_type.variants[1].fields[0].ty.id,
            TypeDefPrimitive::U64,
        );
        assert!(order_type.variants[2].fields.is_empty());
    }

    fn assert_unit_variants(
        types: &scale_info::PortableRegistry,
        type_id: u32,
        expected_names: &[&str],
    ) {
        let ty = types.resolve(type_id).unwrap();
        let TypeDef::Variant(variants) = &ty.type_def else {
            panic!("expected a variant type");
        };
        assert_eq!(
            variants
                .variants
                .iter()
                .map(|variant| variant.name.as_str())
                .collect::<Vec<_>>(),
            expected_names,
        );
        assert!(
            variants
                .variants
                .iter()
                .all(|variant| variant.fields.is_empty())
        );
    }

    fn assert_primitive(
        types: &scale_info::PortableRegistry,
        type_id: u32,
        expected: TypeDefPrimitive,
    ) {
        let ty = types.resolve(type_id).unwrap();
        assert_eq!(ty.type_def, TypeDef::Primitive(expected));
    }

    fn assert_fixed_u8_array(
        types: &scale_info::PortableRegistry,
        type_id: u32,
        expected_len: u32,
        expected_name: Option<&str>,
    ) {
        let ty = types.resolve(type_id).unwrap();
        if let Some(expected_name) = expected_name {
            assert_eq!(
                ty.path.segments.last().map(String::as_str),
                Some(expected_name)
            );
        }
        let TypeDef::Composite(composite) = &ty.type_def else {
            panic!("fixed byte wrapper must be a composite type");
        };
        assert_eq!(composite.fields.len(), 1);
        let bytes = types.resolve(composite.fields[0].ty.id).unwrap();
        let TypeDef::Array(bytes) = &bytes.type_def else {
            panic!("fixed byte wrapper must contain an array");
        };
        assert_eq!(bytes.len, expected_len);
        assert_primitive(types, bytes.type_param.id, TypeDefPrimitive::U8);
    }

    fn assert_option_primitive(
        types: &scale_info::PortableRegistry,
        type_id: u32,
        expected: TypeDefPrimitive,
    ) {
        let ty = types.resolve(type_id).unwrap();
        assert_eq!(ty.path.segments.last().map(String::as_str), Some("Option"));
        let TypeDef::Variant(option) = &ty.type_def else {
            panic!("optional field must use Option");
        };
        assert_eq!(option.variants.len(), 2);
        assert_eq!(option.variants[0].name, "None");
        assert!(option.variants[0].fields.is_empty());
        assert_eq!(option.variants[1].name, "Some");
        assert_eq!(option.variants[1].fields.len(), 1);
        assert_primitive(types, option.variants[1].fields[0].ty.id, expected);
    }

    fn synthetic_error_metadata(second_name: &str, second_index: u8) -> Metadata {
        let RuntimeMetadataPrefixed(_, RuntimeMetadata::V14(mut metadata)) =
            RuntimeMetadataPrefixed::decode(&mut fixture_metadata().as_slice()).unwrap()
        else {
            panic!("Expected V14 fixture metadata");
        };
        let mut pallet = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.error.is_some())
            .unwrap()
            .clone();
        let error_type = pallet.error.as_ref().unwrap().ty.id;
        pallet.name = "SparsePallet".to_string();
        pallet.index = 71;
        metadata.pallets = vec![pallet];
        let ty = metadata
            .types
            .types
            .iter_mut()
            .find(|ty| ty.id == error_type)
            .unwrap();
        let TypeDef::Variant(variants) = &mut ty.ty.type_def else {
            panic!("Expected pallet error variants");
        };
        variants.variants = vec![
            Variant {
                name: "FirstError".to_string(),
                fields: Vec::new(),
                index: 203,
                docs: Vec::new(),
            },
            Variant {
                name: second_name.to_string(),
                fields: Vec::new(),
                index: second_index,
                docs: Vec::new(),
            },
        ];
        let bytes = RuntimeMetadataPrefixed(META_RESERVED, RuntimeMetadata::V14(metadata)).encode();
        metadata::decode_from(&bytes).unwrap()
    }

    #[rstest]
    fn runtime_error_lookup_uses_sparse_declared_indices() {
        let metadata = synthetic_error_metadata("SecondError", 9);
        let catalog = DeepXRuntimeInterfaceCatalog::from_metadata(&metadata).unwrap();

        for (index, name) in [(203, "FirstError"), (9, "SecondError")] {
            let (pallet, error) = catalog.error_by_index(71, index).unwrap();
            assert_eq!(pallet.name(), "SparsePallet");
            assert_eq!(pallet.index(), 71);
            assert_eq!(error.name(), name);
            assert_eq!(error.index(), index);
        }

        assert_eq!(
            catalog.error_by_index(0, 203),
            Err(DeepXRuntimeInterfaceError::PalletIndexUnavailable(0)),
        );

        for index in [0, 1, 255] {
            assert_eq!(
                catalog.error_by_index(71, index),
                Err(DeepXRuntimeInterfaceError::ErrorIndexUnavailable(71, index)),
            );
        }
    }

    #[rstest]
    #[case("FirstError", 9)]
    #[case("SecondError", 203)]
    fn runtime_interface_rejects_duplicate_error_identity(
        #[case] second_name: &str,
        #[case] second_index: u8,
    ) {
        let metadata = synthetic_error_metadata(second_name, second_index);
        assert_eq!(
            DeepXRuntimeInterfaceCatalog::from_metadata(&metadata),
            Err(DeepXRuntimeInterfaceError::DuplicateError(
                "SparsePallet".to_string(),
                second_name.to_string(),
            )),
        );
    }

    #[rstest]
    fn runtime_error_identities_match_independently_decoded_fixture() {
        let bytes = fixture_metadata();
        let genesis = decode_32(TESTNET_SPEC_376_GENESIS_HASH).unwrap();
        let runtime =
            validate_testnet_runtime(&DeepXEnvironment::Testnet, genesis, 376, 1, &bytes).unwrap();
        let mut input = bytes.as_slice();
        let RuntimeMetadataPrefixed(prefix, RuntimeMetadata::V14(metadata)) =
            RuntimeMetadataPrefixed::decode(&mut input).unwrap()
        else {
            panic!("Expected V14 fixture metadata");
        };
        assert_eq!(prefix, META_RESERVED);
        assert!(input.is_empty());
        let catalog = runtime.interfaces();
        assert_eq!(catalog.pallets().len(), metadata.pallets.len());
        let mut error_count = 0;

        for pallet in &metadata.pallets {
            let actual_pallet = catalog.pallet(&pallet.name).unwrap();
            assert_eq!(actual_pallet.index(), pallet.index);
            let Some(errors) = &pallet.error else {
                assert!(actual_pallet.errors().is_empty());
                assert_eq!(
                    catalog.error_by_index(pallet.index, 0),
                    Err(DeepXRuntimeInterfaceError::ErrorIndexUnavailable(
                        pallet.index,
                        0,
                    )),
                );
                continue;
            };
            let TypeDef::Variant(variants) =
                &metadata.types.resolve(errors.ty.id).unwrap().type_def
            else {
                panic!("Expected pallet error variants");
            };
            assert_eq!(actual_pallet.errors().len(), variants.variants.len());

            for (actual, expected) in actual_pallet.errors().iter().zip(&variants.variants) {
                assert_eq!(actual.name(), expected.name);
                assert_eq!(actual.index(), expected.index);
                let (found_pallet, found_error) = catalog
                    .error_by_index(pallet.index, expected.index)
                    .unwrap();
                assert!(std::ptr::eq(found_pallet, actual_pallet));
                assert!(std::ptr::eq(found_error, actual));
                error_count += 1;
            }

            let unknown_error = (0..=u8::MAX)
                .find(|index| {
                    !variants
                        .variants
                        .iter()
                        .any(|variant| variant.index == *index)
                })
                .unwrap();
            assert_eq!(
                catalog.error_by_index(pallet.index, unknown_error),
                Err(DeepXRuntimeInterfaceError::ErrorIndexUnavailable(
                    pallet.index,
                    unknown_error,
                )),
            );
        }

        assert!(error_count > 0, "Fixture must contain pallet errors");
        let unknown_pallet = (0..=u8::MAX)
            .find(|index| !metadata.pallets.iter().any(|pallet| pallet.index == *index))
            .unwrap();
        assert_eq!(
            catalog.error_by_index(unknown_pallet, 0),
            Err(DeepXRuntimeInterfaceError::PalletIndexUnavailable(
                unknown_pallet,
            )),
        );
    }
}
