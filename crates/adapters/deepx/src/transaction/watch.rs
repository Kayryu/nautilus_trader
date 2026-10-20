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

//! Fail-closed DeepX canonical block and submission-pool observations.

use std::{collections::HashSet, time::Duration};

use nautilus_blockchain::rpc::http::BlockchainHttpRpcClient;
use nautilus_core::hex;
use parity_scale_codec::{Compact, Decode, Encode};
use scale_info::{TypeDef, TypeDefPrimitive};
use serde::Deserialize;
use serde_json::json;
use subxt_core::{
    config::{Hasher, substrate::BlakeTwo256},
    events::{Events, Phase},
    ext::scale_value::{Composite, Value as ScaleValue, ValueDef},
};
use thiserror::Error;

use super::{
    DeepXBusinessEventOutcome, DeepXCanonicalBlockEvidence, DeepXDispatchOutcome,
    DeepXInclusionEvidence, DeepXInclusionEvidenceError, DeepXIndexedOutcome,
    DeepXMissedBlockScanPlan, DeepXRecoveryScan, DeepXRecoveryScanCollectionError,
    DeepXRecoveryScanCollector, DeepXRecoveryScanPlanError, DeepXReorganizationDecision,
    DeepXSubmissionPoolEvidence, DeepXTransactionIdentity, DeepXTransactionOperation,
    classify_reorganization, plan_missed_block_scan,
};
use crate::{
    common::{
        DeepXEnvironment, DeepXPrivateKey,
        parse::{decimal_to_scaled_i128, parse_canonical_order_id},
    },
    config::{DeepXRpcRole, DeepXValidatedRpcEndpoints},
    http::{
        DeepXDelegateMode, DeepXPerpAccountTradeRecord, DeepXPerpMarketLookup,
        DeepXPerpWalletTradeMarket, DeepXSpotAccountTradeRecord,
    },
    rpc::{
        DEEPX_RECOVERY_RPC_METHODS, DEEPX_SUBMISSION_RPC_METHODS, DEEPX_WATCH_RPC_METHODS,
        DeepXHistoricalRuntimeError, DeepXValidatedRpcMethodCapabilities,
    },
    signing::{
        ApprovedRuntimeIdentity, DeepXPerpModifyParams, DeepXReadOnlyRuntimeSnapshot,
        DeepXRemoveDelegateAccountParams, DeepXRuntimeConfig, DeepXRuntimeInterfaceCatalog,
        DeepXRuntimeInterfaceError, DeepXRuntimeSnapshotPermit, DeepXSetDelegateAccountParams,
        DeepXUpdateDelegateModeParams, DeepXWritableDelegateMode, RuntimeSnapshot, SigningError,
        SnapshotError, sign_remove_delegate_account, sign_set_delegate_account,
        sign_update_delegate_mode,
    },
};

const SYSTEM_EVENTS_STORAGE_KEY: &str = concat!(
    "0x26aa394eea5630e07c48ae0c9558cef7",
    "80d41e5e16056765bc8461851072c9d7",
);

async fn execute_rpc_read_with_retry<T: for<'de> Deserialize<'de>>(
    client: &BlockchainHttpRpcClient,
    request: serde_json::Value,
) -> anyhow::Result<T> {
    for delay_ms in [250, 500] {
        match client.execute_rpc_call(request.clone()).await {
            Ok(value) => return Ok(value),
            Err(_) => tokio::time::sleep(Duration::from_millis(delay_ms)).await,
        }
    }
    client.execute_rpc_call(request).await
}

async fn execute_optional_rpc_read_with_retry<T: for<'de> Deserialize<'de>>(
    client: &BlockchainHttpRpcClient,
    request: serde_json::Value,
) -> anyhow::Result<Option<T>> {
    for delay_ms in [250, 500] {
        match client.execute_optional_rpc_call(request.clone()).await {
            Ok(Some(value)) => return Ok(Some(value)),
            Ok(None) | Err(_) => tokio::time::sleep(Duration::from_millis(delay_ms)).await,
        }
    }
    client.execute_optional_rpc_call(request).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoricalRuntimeVersion {
    spec_version: u32,
    transaction_version: u32,
}

async fn observe_historical_read_only_runtime(
    environment: &DeepXEnvironment,
    genesis_hash: [u8; 32],
    watch_url: &str,
    block_hash: [u8; 32],
) -> Result<DeepXReadOnlyRuntimeSnapshot, DeepXHistoricalRuntimeError> {
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    let encoded_hash = format!("0x{}", hex::encode(block_hash));
    let version: HistoricalRuntimeVersion = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "state_getRuntimeVersion", "params": [encoded_hash],
        }))
        .await
        .map_err(|source| DeepXHistoricalRuntimeError::Rpc {
            method: "state_getRuntimeVersion",
            source,
        })?;
    let encoded_metadata: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "state_getMetadata", "params": [encoded_hash],
        }))
        .await
        .map_err(|source| DeepXHistoricalRuntimeError::Rpc {
            method: "state_getMetadata",
            source,
        })?;
    let metadata = encoded_metadata
        .strip_prefix("0x")
        .filter(|value| !value.is_empty())
        .and_then(|value| hex::decode(value).ok())
        .ok_or(DeepXHistoricalRuntimeError::InvalidMetadata)?;
    DeepXReadOnlyRuntimeSnapshot::observed_testnet(
        environment,
        genesis_hash,
        version.spec_version,
        version.transaction_version,
        &metadata,
    )
    .map_err(DeepXHistoricalRuntimeError::from)
}

trait RuntimeMetadataView {
    fn identity(&self) -> &ApprovedRuntimeIdentity;
    fn interfaces(&self) -> &DeepXRuntimeInterfaceCatalog;
    fn metadata(&self) -> &subxt_core::metadata::Metadata;
}

impl RuntimeMetadataView for RuntimeSnapshot {
    fn identity(&self) -> &ApprovedRuntimeIdentity {
        self.identity()
    }

    fn interfaces(&self) -> &DeepXRuntimeInterfaceCatalog {
        self.interfaces()
    }

    fn metadata(&self) -> &subxt_core::metadata::Metadata {
        self.metadata()
    }
}

impl RuntimeMetadataView for DeepXReadOnlyRuntimeSnapshot {
    fn identity(&self) -> &ApprovedRuntimeIdentity {
        self.identity()
    }

    fn interfaces(&self) -> &DeepXRuntimeInterfaceCatalog {
        self.interfaces()
    }

    fn metadata(&self) -> &subxt_core::metadata::Metadata {
        self.metadata()
    }
}

impl<T: RuntimeMetadataView + ?Sized> RuntimeMetadataView for &T {
    fn identity(&self) -> &ApprovedRuntimeIdentity {
        (*self).identity()
    }

    fn interfaces(&self) -> &DeepXRuntimeInterfaceCatalog {
        (*self).interfaces()
    }

    fn metadata(&self) -> &subxt_core::metadata::Metadata {
        (*self).metadata()
    }
}

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

/// Failures decoding complete delegate account event storage.
#[derive(Debug, Error)]
pub enum DeepXDelegateAccountEventError {
    /// The approved runtime does not expose the required account events.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE decoding failed against approved metadata.
    #[error("unable to decode DeepX delegate account events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The event count or encoded length is inconsistent.
    #[error("DeepX delegate account evidence is not complete System.Events")]
    MalformedEventBytes,
    /// A delegate account event violates the approved field or phase contract.
    #[error("invalid DeepX delegate account event at index {0}")]
    InvalidChange(u32),
}

/// Failures binding a delegate account event to one finalized extrinsic.
#[derive(Debug, Error)]
pub enum DeepXDelegateAccountVerificationError {
    /// Complete event storage could not be decoded.
    #[error(transparent)]
    Events(#[from] DeepXDelegateAccountEventError),
    /// The runtime does not expose the required dispatch events.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// Dispatch event decoding failed against approved metadata.
    #[error("unable to decode DeepX delegate account dispatch events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The target extrinsic has no unique successful dispatch event.
    #[error("DeepX delegate account extrinsic has no unique successful dispatch")]
    Dispatch,
    /// The target extrinsic has no unique expected account event.
    #[error("DeepX delegate account extrinsic has no unique expected event")]
    Change,
    /// The event differs from the expected owner, delegate, or operation.
    #[error("DeepX delegate account event conflicts with expected terms")]
    ConflictingChange,
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

/// Failures decoding complete delegate-mode event storage.
#[derive(Debug, Error)]
pub enum DeepXDelegateModeEventError {
    /// The approved runtime does not expose the mode-change event.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE decoding failed against approved metadata.
    #[error("unable to decode DeepX delegate mode events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The event count or encoded length is inconsistent.
    #[error("DeepX delegate mode evidence is not complete System.Events")]
    MalformedEventBytes,
    /// A mode-change event violates the approved field or phase contract.
    #[error("invalid DeepX delegate mode event at index {0}")]
    InvalidChange(u32),
}

/// Failures binding one delegate mode change to an exact finalized extrinsic location.
#[derive(Debug, Error)]
pub enum DeepXDelegateModeVerificationError {
    /// The complete event storage could not be decoded.
    #[error(transparent)]
    Events(#[from] DeepXDelegateModeEventError),
    /// Dispatch event decoding failed against approved metadata.
    #[error("unable to decode DeepX delegate mode dispatch events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The runtime does not expose the required dispatch events.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The target extrinsic has no unique successful dispatch event.
    #[error("DeepX delegate mode extrinsic has no unique successful dispatch")]
    Dispatch,
    /// The target extrinsic has no unique mode-change event.
    #[error("DeepX delegate mode extrinsic has no unique mode change")]
    Change,
    /// The mode-change event differs from the expected wallet, delegate, or mode.
    #[error("DeepX delegate mode change conflicts with expected terms")]
    ConflictingChange,
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

/// Failures decoding complete Spot trade event storage.
#[derive(Debug, Error)]
pub enum DeepXSpotTradeEventError {
    /// The approved runtime does not expose the Spot match event.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE decoding failed against approved metadata.
    #[error("unable to decode DeepX Spot trade events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The event sequence was truncated or contained trailing bytes.
    #[error("DeepX Spot trade evidence is not complete System.Events")]
    MalformedEventBytes,
    /// An event violates the approved runtime field contract.
    #[error("invalid DeepX finalized Spot trade event at index {0}")]
    InvalidTrade(u32),
}

/// Failures decoding complete finalized perpetual trade events.
#[derive(Debug, Error)]
pub enum DeepXPerpTradeEventError {
    /// The approved runtime does not expose the trade event.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// SCALE event decoding failed against approved metadata.
    #[error("unable to decode DeepX perpetual trade events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// The event sequence was truncated or contained trailing bytes.
    #[error("DeepX perpetual trade evidence is not complete System.Events")]
    MalformedEventBytes,
    /// A trade event violates the runtime field or block identity contract.
    #[error("invalid DeepX finalized perpetual trade event at index {0}")]
    InvalidTrade(u32),
}

/// Failures binding one REST account execution to finalized chain trade evidence.
#[derive(Debug, Error)]
pub enum DeepXPerpTradeMatchError {
    /// The REST order ID or market scale cannot be used for exact matching.
    #[error("invalid DeepX perpetual account trade identity or scale")]
    InvalidInput,
    /// A REST financial value cannot be represented in the chain's integer units.
    #[error("inexact DeepX perpetual account trade {field}: {source}")]
    InexactValue {
        /// Financial field that could not be converted exactly.
        field: &'static str,
        /// Exact decimal conversion failure.
        #[source]
        source: crate::common::DeepXError,
    },
    /// No finalized trade agrees with every account, order, and financial field.
    #[error("DeepX account trade has no matching finalized trade event")]
    NotFound,
    /// More than one finalized event agrees with the same REST execution.
    #[error("DeepX account trade matches multiple finalized trade events")]
    Ambiguous,
}

/// Matches one REST account trade to one decoded event, including its venue trade ID.
///
/// The caller supplies the validated account and market's base/quote decimals and quote symbol.
/// Runtime prices use the chain's fixed six-decimal quote precision and leverage uses three
/// decimals. The REST ID encodes the block number times one million plus the global event index.
/// The caller must independently establish canonical finality
/// for the event's block; a match does not prove the REST timestamp.
///
/// # Errors
///
/// Returns an error for malformed inputs, inexact values, no match, or multiple exact matches.
pub fn match_finalized_perp_account_trade<'a>(
    trades: &'a [DeepXFinalizedPerpTrade],
    record: &DeepXPerpAccountTradeRecord,
    subaccount: [u8; 20],
    base_decimal: u8,
    quote_decimal: u8,
    quote_symbol: &str,
) -> Result<&'a DeepXFinalizedPerpTrade, DeepXPerpTradeMatchError> {
    use DeepXPerpTradeMatchError as Error;
    if record.id == 0
        || record.market_id == 0
        || base_decimal > 28
        || quote_decimal > 28
        || quote_symbol.is_empty()
    {
        return Err(Error::InvalidInput);
    }
    let order_id = parse_canonical_order_id(&record.order_id).ok_or(Error::InvalidInput)?;
    if record.price <= rust_decimal::Decimal::ZERO
        || record.size <= rust_decimal::Decimal::ZERO
        || record.leverage <= rust_decimal::Decimal::ZERO
        || !matches!(record.taker.as_str(), "Buyer" | "Seller")
        || !matches!(record.filled_direction.as_str(), "Long" | "Short" | "Both")
        || !record.fee_asset.is_empty() && !record.fee_asset.eq_ignore_ascii_case(quote_symbol)
    {
        return Err(Error::InvalidInput);
    }
    let price = decimal_to_scaled_i128(record.price, 6).map_err(|source| Error::InexactValue {
        field: "price",
        source,
    })?;
    let size = decimal_to_scaled_i128(record.size, u32::from(base_decimal)).map_err(|source| {
        Error::InexactValue {
            field: "size",
            source,
        }
    })?;
    let leverage =
        decimal_to_scaled_i128(record.leverage, 3).map_err(|source| Error::InexactValue {
            field: "leverage",
            source,
        })?;
    let fee = decimal_to_scaled_i128(record.fee, u32::from(quote_decimal)).map_err(|source| {
        Error::InexactValue {
            field: "fee",
            source,
        }
    })?;
    let mut matching = trades.iter().filter(|trade| {
        let (owner, venue_order_id, raw_leverage, raw_fee) = if record.is_long {
            (
                trade.buyer,
                trade.buyer_order_id,
                trade.buyer_leverage,
                if trade.taker == "Buyer" {
                    trade.taker_fee
                } else {
                    trade.maker_fee
                },
            )
        } else {
            (
                trade.seller,
                trade.seller_order_id,
                trade.seller_leverage,
                if trade.taker == "Seller" {
                    trade.taker_fee
                } else {
                    trade.maker_fee
                },
            )
        };
        trade.event_index < 1_000_000
            && trade
                .block_number
                .checked_mul(1_000_000)
                .and_then(|base| base.checked_add(u64::from(trade.event_index)))
                == Some(record.id)
            && trade.market_id as u64 == record.market_id
            && owner == subaccount
            && venue_order_id == order_id
            && trade.taker == record.taker
            && trade.filled_direction == record.filled_direction
            && trade
                .fee_asset
                .eq_ignore_ascii_case(quote_symbol.as_bytes())
            && i128::try_from(trade.price) == Ok(price)
            && i128::try_from(trade.size) == Ok(size)
            && i128::from(raw_leverage) == leverage
            && raw_fee.checked_neg() == Some(fee)
    });
    let matched = matching.next().ok_or(Error::NotFound)?;
    if matching.next().is_some() {
        return Err(Error::Ambiguous);
    }
    Ok(matched)
}

/// Failures binding one Spot REST execution to finalized `OrderMatched` evidence.
#[derive(Debug, Error)]
pub enum DeepXSpotTradeMatchError {
    /// Market identity, side, or exact market scale is invalid.
    #[error("invalid DeepX Spot account trade identity or scale")]
    InvalidInput,
    /// A REST value cannot be represented exactly in raw runtime units.
    #[error("inexact DeepX Spot account trade {field}: {source}")]
    InexactValue {
        /// Financial field that could not be converted exactly.
        field: &'static str,
        /// Exact decimal conversion failure.
        #[source]
        source: crate::common::DeepXError,
    },
    /// No finalized event agrees with all REST fields.
    #[error("DeepX Spot account trade has no matching finalized event")]
    NotFound,
    /// More than one event agrees with one REST trade.
    #[error("DeepX Spot account trade matches multiple finalized events")]
    Ambiguous,
}

/// Matches one Spot REST trade to a supplied finalized event and its global event index.
///
/// The caller validates the REST response's account request and independently establishes
/// canonical finality of each event block. The six-decimal runtime price and event-ID formula
/// follow the approved node source and the REST trade-ID example; this match does not prove
/// the REST timestamp or historical completeness.
///
/// # Errors
///
/// Returns an error for invalid market scales, inexact values, or missing or ambiguous evidence.
pub fn match_finalized_spot_account_trade<'a>(
    trades: &'a [DeepXFinalizedSpotTrade],
    record: &DeepXSpotAccountTradeRecord,
    subaccount: [u8; 20],
    base_decimal: u8,
    quote_decimal: u8,
    base_symbol: &str,
    quote_symbol: &str,
) -> Result<&'a DeepXFinalizedSpotTrade, DeepXSpotTradeMatchError> {
    use DeepXSpotTradeMatchError as Error;

    if record.id == 0
        || base_decimal > 28
        || quote_decimal > 28
        || base_symbol.is_empty()
        || quote_symbol.is_empty()
        || !matches!(record.order_side.as_str(), "Buy" | "Sell")
        || !matches!(record.taker.as_str(), "Buyer" | "Seller")
    {
        return Err(Error::InvalidInput);
    }
    let pair = record
        .pair
        .strip_prefix("0x")
        .and_then(|encoded| hex::decode_array::<32>(encoded).ok())
        .ok_or(Error::InvalidInput)?;
    let order_id = parse_canonical_order_id(&record.order_id).ok_or(Error::InvalidInput)?;
    if record.price <= rust_decimal::Decimal::ZERO
        || record.base_amount <= rust_decimal::Decimal::ZERO
        || record.quote_amount <= rust_decimal::Decimal::ZERO
    {
        return Err(Error::InvalidInput);
    }
    let scaled = |value, field, decimals| {
        decimal_to_scaled_i128(value, decimals)
            .map_err(|source| Error::InexactValue { field, source })
    };
    let price = scaled(record.price, "price", 6)?;
    let base_amount = scaled(record.base_amount, "base amount", u32::from(base_decimal))?;
    let quote_amount = scaled(
        record.quote_amount,
        "quote amount",
        u32::from(quote_decimal),
    )?;
    let fee_decimals = if record.order_side == "Buy" {
        base_decimal
    } else {
        quote_decimal
    };
    let fee = scaled(record.fee, "fee", u32::from(fee_decimals))?;
    let expected_fee_asset = if record.order_side == "Buy" {
        base_symbol
    } else {
        quote_symbol
    };
    if !record.fee_asset.is_empty() && !record.fee_asset.eq_ignore_ascii_case(expected_fee_asset) {
        return Err(Error::InvalidInput);
    }
    let as_u256 = |value| {
        let value = u128::try_from(value).ok()?;
        let mut bytes = [0_u8; 32];
        bytes[..16].copy_from_slice(&value.to_le_bytes());
        Some(bytes)
    };
    let (price, base_amount, quote_amount) = (
        as_u256(price).ok_or(Error::InvalidInput)?,
        as_u256(base_amount).ok_or(Error::InvalidInput)?,
        as_u256(quote_amount).ok_or(Error::InvalidInput)?,
    );
    let mut matching = trades.iter().filter(|trade| {
        let (owner, venue_order_id, raw_fee, fee_asset, other_fee_asset, other_symbol) =
            if record.order_side == "Buy" {
                (
                    trade.buyer,
                    trade.buy_id,
                    if trade.taker == "Buyer" {
                        trade.taker_fee
                    } else {
                        trade.maker_fee
                    },
                    if trade.taker == "Buyer" {
                        &trade.taker_fee_asset
                    } else {
                        &trade.maker_fee_asset
                    },
                    if trade.taker == "Buyer" {
                        &trade.maker_fee_asset
                    } else {
                        &trade.taker_fee_asset
                    },
                    quote_symbol,
                )
            } else {
                (
                    trade.seller,
                    trade.sell_id,
                    if trade.taker == "Seller" {
                        trade.taker_fee
                    } else {
                        trade.maker_fee
                    },
                    if trade.taker == "Seller" {
                        &trade.taker_fee_asset
                    } else {
                        &trade.maker_fee_asset
                    },
                    if trade.taker == "Seller" {
                        &trade.maker_fee_asset
                    } else {
                        &trade.taker_fee_asset
                    },
                    base_symbol,
                )
            };
        trade.event_index < 1_000_000
            && trade
                .block_number
                .checked_mul(1_000_000)
                .and_then(|base| base.checked_add(u64::from(trade.event_index)))
                == Some(record.id)
            && trade.pair == pair
            && owner == subaccount
            && venue_order_id == order_id
            && trade.taker == record.taker
            && trade.price == price
            && trade.trade_amount_b == base_amount
            && trade.trade_amount_u == quote_amount
            && fee_asset.eq_ignore_ascii_case(expected_fee_asset.as_bytes())
            && other_fee_asset.eq_ignore_ascii_case(other_symbol.as_bytes())
            && raw_fee.checked_neg() == Some(fee)
    });
    let matched = matching.next().ok_or(Error::NotFound)?;
    if matching.next().is_some() {
        return Err(Error::Ambiguous);
    }
    Ok(matched)
}

/// One wallet account trade matched to a finalized perpetual event.
#[derive(Clone, Copy, Debug)]
pub struct DeepXFinalizedWalletPerpTradeMatch<'a> {
    /// Wallet subaccount on the event's buyer or seller side.
    pub subaccount: [u8; 20],
    /// Complete finalized event evidence.
    pub trade: &'a DeepXFinalizedPerpTrade,
    /// Mutable REST account record whose financial fields match the event.
    pub record: &'a DeepXPerpAccountTradeRecord,
}

/// Failures binding a complete wallet trade page to finalized perpetual events.
#[derive(Debug, Error)]
pub enum DeepXFinalizedWalletPerpTradeMatchError {
    /// The supplied event slice mixes blocks or repeats an event index.
    #[error("DeepX finalized wallet trade events have conflicting block identity")]
    ConflictingEvents,
    /// The directory repeats a subaccount or supplies no subaccounts.
    #[error("DeepX wallet subaccount directory is empty or contains duplicates")]
    InvalidSubaccounts,
    /// Both sides of an event belong to one subaccount, which the REST identity cannot distinguish.
    #[error("DeepX self-trade cannot be bound to one account trade record")]
    SelfTrade,
    /// Exactly one market group, subaccount group, or metadata record was not available.
    #[error("DeepX finalized wallet trade market or subaccount identity is unavailable")]
    MissingIdentity,
    /// A current market identity disagrees with the REST wallet group.
    #[error("DeepX finalized wallet trade market identity conflicts with metadata")]
    MarketIdentityMismatch,
    /// An involved group has more pages, so absence in the observed page is not authoritative.
    #[error("DeepX finalized wallet trade group has an unresolved continuation cursor")]
    IncompletePage,
    /// The event's venue ID was absent from the complete observed group.
    #[error("DeepX finalized wallet trade event is absent from the wallet page")]
    MissingRecord,
    /// The same event ID appeared more than once in its wallet group.
    #[error("DeepX finalized wallet trade event has duplicate REST records")]
    DuplicateRecord,
    /// An account record disagrees with the event's exact fields.
    #[error(transparent)]
    Trade(#[from] DeepXPerpTradeMatchError),
}

/// Matches every event involving a supplied wallet subaccount against complete REST groups.
///
/// The caller must independently validate wallet ownership and obtain finalized events. Market
/// metadata and wallet pages are current, mutable REST observations; this match does not prove a
/// historical wallet directory, REST timestamp, or cryptographic storage inclusion.
///
/// # Errors
///
/// Returns an error without partial matches for conflicting identity, incomplete pages, missing
/// records, duplicate records, or exact financial mismatches.
pub fn match_finalized_wallet_perp_trades<'a>(
    trades: &'a [DeepXFinalizedPerpTrade],
    groups: &'a [DeepXPerpWalletTradeMarket],
    subaccounts: &[[u8; 20]],
    markets: &[DeepXPerpMarketLookup],
) -> Result<Vec<DeepXFinalizedWalletPerpTradeMatch<'a>>, DeepXFinalizedWalletPerpTradeMatchError> {
    use DeepXFinalizedWalletPerpTradeMatchError as Error;

    if subaccounts.is_empty()
        || subaccounts.iter().copied().collect::<HashSet<_>>().len() != subaccounts.len()
    {
        return Err(Error::InvalidSubaccounts);
    }
    if let Some(first) = trades.first() {
        let mut indices = HashSet::new();
        for trade in trades {
            if trade.block_hash != first.block_hash
                || trade.block_number != first.block_number
                || !indices.insert(trade.event_index)
            {
                return Err(Error::ConflictingEvents);
            }
        }
    }

    let mut matched = Vec::new();
    for trade in trades {
        for subaccount in subaccounts {
            if trade.buyer != *subaccount && trade.seller != *subaccount {
                continue;
            }
            if trade.buyer == *subaccount && trade.seller == *subaccount {
                return Err(Error::SelfTrade);
            }
            let mut matching_groups = groups
                .iter()
                .filter(|group| group.market_id == u64::from(trade.market_id));
            let group = matching_groups.next().ok_or(Error::MissingIdentity)?;
            if matching_groups.next().is_some() {
                return Err(Error::MarketIdentityMismatch);
            }
            let mut matching_markets = markets.iter().filter(|market| market.id == group.market_id);
            let market = matching_markets.next().ok_or(Error::MissingIdentity)?;
            if matching_markets.next().is_some() || market.name != group.market_name {
                return Err(Error::MarketIdentityMismatch);
            }
            let encoded_subaccount = format!("0x{}", hex::encode(subaccount));
            let mut matching_subaccounts = group.subaccounts.iter().filter(|candidate| {
                candidate
                    .subaccount
                    .eq_ignore_ascii_case(&encoded_subaccount)
            });
            let account_group = matching_subaccounts.next().ok_or(Error::MissingIdentity)?;
            if matching_subaccounts.next().is_some() {
                return Err(Error::MissingIdentity);
            }
            if account_group.trades.has_next {
                return Err(Error::IncompletePage);
            }
            let expected_id = trade
                .block_number
                .checked_mul(1_000_000)
                .and_then(|base| base.checked_add(u64::from(trade.event_index)))
                .ok_or(Error::MissingRecord)?;
            let mut records = account_group
                .trades
                .items
                .iter()
                .filter(|record| record.id == expected_id);
            let record = records.next().ok_or(Error::MissingRecord)?;
            if records.next().is_some() {
                return Err(Error::DuplicateRecord);
            }
            let confirmed = match_finalized_perp_account_trade(
                trades,
                record,
                *subaccount,
                market.base_decimal,
                market.quote_decimal,
                &market.quote_symbol,
            )?;
            if confirmed.event_index != trade.event_index {
                return Err(Error::ConflictingEvents);
            }
            matched.push(DeepXFinalizedWalletPerpTradeMatch {
                subaccount: *subaccount,
                trade: confirmed,
                record,
            });
        }
    }
    Ok(matched)
}

/// Decodes delegate setup and removal from one complete finalized `System.Events` value.
///
/// The events identify owner and delegate only. Setup labels, expiry, and creation versus renewal
/// must be established through the exact signed call and any subsequent state observation.
/// The caller independently establishes canonical finality for the supplied block identity.
///
/// # Errors
///
/// Returns an error for unsupported metadata, incomplete SCALE bytes, or invalid event fields.
pub fn decode_finalized_delegate_account_changes(
    snapshot: &RuntimeSnapshot,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedDelegateAccountChange>, DeepXDelegateAccountEventError> {
    decode_finalized_delegate_account_changes_with_runtime(
        snapshot,
        block_hash,
        block_number,
        event_bytes,
    )
}

fn decode_finalized_delegate_account_changes_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedDelegateAccountChange>, DeepXDelegateAccountEventError> {
    snapshot
        .interfaces()
        .event("Subaccount", "DelegateAccountSet")?;
    snapshot
        .interfaces()
        .event("Subaccount", "DelegateAccountRemoved")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXDelegateAccountEventError::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut changes = Vec::new();
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    for event in events.iter() {
        let event = event.map_err(|e| DeepXDelegateAccountEventError::Decode(Box::new(e)))?;
        let event_index = count;
        count = count
            .checked_add(1)
            .ok_or(DeepXDelegateAccountEventError::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(DeepXDelegateAccountEventError::MalformedEventBytes)?;
        if event.pallet_name() != "Subaccount" {
            continue;
        }
        let kind = match event.variant_name() {
            "DelegateAccountSet" => DeepXDelegateAccountChangeKind::Set,
            "DelegateAccountRemoved" => DeepXDelegateAccountChangeKind::Removed,
            _ => continue,
        };
        let Phase::ApplyExtrinsic(extrinsic_index) = event.phase() else {
            return Err(DeepXDelegateAccountEventError::InvalidChange(event_index));
        };
        let fields = event
            .field_values()
            .map_err(|e| DeepXDelegateAccountEventError::Decode(Box::new(e)))?;
        let Composite::Named(fields) = fields else {
            return Err(DeepXDelegateAccountEventError::InvalidChange(event_index));
        };
        if fields.len() != 2 {
            return Err(DeepXDelegateAccountEventError::InvalidChange(event_index));
        }
        let field = |name| {
            fields
                .iter()
                .find_map(|(candidate, value)| (candidate == name).then_some(value))
        };
        let owner = field("owner")
            .and_then(decode_event_bytes)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(DeepXDelegateAccountEventError::InvalidChange(event_index))?;
        let delegate = field("delegate")
            .and_then(decode_event_bytes)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(DeepXDelegateAccountEventError::InvalidChange(event_index))?;
        changes.push(DeepXFinalizedDelegateAccountChange {
            block_hash,
            block_number,
            event_index,
            extrinsic_index,
            owner,
            delegate,
            kind,
        });
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(DeepXDelegateAccountEventError::MalformedEventBytes);
    }
    Ok(changes)
}

/// Binds a delegate setup or removal and successful dispatch to one finalized location.
///
/// The caller supplies complete `System.Events` read at `location.block_hash()` and separately
/// proves that the located signed extrinsic encodes the expected account call.
///
/// # Errors
///
/// Returns an error for incomplete events, missing or duplicate same-index evidence, failed
/// dispatch, or an event that differs from the expected identities or operation.
pub fn verify_finalized_delegate_account_change_events(
    snapshot: &RuntimeSnapshot,
    location: DeepXFinalizedTransactionLocation,
    event_bytes: &[u8],
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_kind: DeepXDelegateAccountChangeKind,
) -> Result<DeepXFinalizedDelegateAccountChange, DeepXDelegateAccountVerificationError> {
    verify_finalized_delegate_account_change_events_with_runtime(
        snapshot,
        location,
        event_bytes,
        expected_owner,
        expected_delegate,
        expected_kind,
    )
}

fn verify_finalized_delegate_account_change_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    location: DeepXFinalizedTransactionLocation,
    event_bytes: &[u8],
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_kind: DeepXDelegateAccountChangeKind,
) -> Result<DeepXFinalizedDelegateAccountChange, DeepXDelegateAccountVerificationError> {
    snapshot.interfaces().event("System", "ExtrinsicSuccess")?;
    snapshot.interfaces().event("System", "ExtrinsicFailed")?;
    let changes = decode_finalized_delegate_account_changes_with_runtime(
        snapshot,
        location.block_hash(),
        location.block_number(),
        event_bytes,
    )?;
    let mut change = None;
    for candidate in changes {
        if candidate.extrinsic_index != location.extrinsic_index() {
            continue;
        }
        if candidate.owner != expected_owner
            || candidate.delegate != expected_delegate
            || candidate.kind != expected_kind
        {
            return Err(DeepXDelegateAccountVerificationError::ConflictingChange);
        }
        if change.replace(candidate).is_some() {
            return Err(DeepXDelegateAccountVerificationError::Change);
        }
    }
    let mut dispatch = None;
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    for event in events.iter() {
        let event =
            event.map_err(|e| DeepXDelegateAccountVerificationError::Decode(Box::new(e)))?;
        if event.phase() != Phase::ApplyExtrinsic(location.extrinsic_index())
            || event.pallet_name() != "System"
        {
            continue;
        }
        let outcome = match event.variant_name() {
            "ExtrinsicSuccess" => DeepXDispatchOutcome::Success,
            "ExtrinsicFailed" => DeepXDispatchOutcome::Failed,
            _ => continue,
        };
        if dispatch.replace(outcome).is_some() {
            return Err(DeepXDelegateAccountVerificationError::Dispatch);
        }
    }
    if dispatch != Some(DeepXDispatchOutcome::Success) {
        return Err(DeepXDelegateAccountVerificationError::Dispatch);
    }
    change.ok_or(DeepXDelegateAccountVerificationError::Change)
}

/// Decodes delegate mode changes from one complete finalized `System.Events` value.
///
/// The caller independently establishes canonical finality for the supplied block identity and
/// binds an observed extrinsic to its signed call. This decoder alone grants no management result.
///
/// # Errors
///
/// Returns an error for unsupported metadata, incomplete SCALE bytes, or invalid event fields.
pub fn decode_finalized_delegate_mode_changes(
    snapshot: &RuntimeSnapshot,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedDelegateModeChange>, DeepXDelegateModeEventError> {
    decode_finalized_delegate_mode_changes_with_runtime(
        snapshot,
        block_hash,
        block_number,
        event_bytes,
    )
}

fn decode_finalized_delegate_mode_changes_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedDelegateModeChange>, DeepXDelegateModeEventError> {
    snapshot
        .interfaces()
        .event("Subaccount", "DelegateAccountModeUpdated")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXDelegateModeEventError::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut changes = Vec::new();
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    for event in events.iter() {
        let event = event.map_err(|e| DeepXDelegateModeEventError::Decode(Box::new(e)))?;
        let event_index = count;
        count = count
            .checked_add(1)
            .ok_or(DeepXDelegateModeEventError::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(DeepXDelegateModeEventError::MalformedEventBytes)?;
        if event.pallet_name() != "Subaccount"
            || event.variant_name() != "DelegateAccountModeUpdated"
        {
            continue;
        }
        let Phase::ApplyExtrinsic(extrinsic_index) = event.phase() else {
            return Err(DeepXDelegateModeEventError::InvalidChange(event_index));
        };
        let fields = event
            .field_values()
            .map_err(|e| DeepXDelegateModeEventError::Decode(Box::new(e)))?;
        let Composite::Named(fields) = fields else {
            return Err(DeepXDelegateModeEventError::InvalidChange(event_index));
        };
        let field = |name| {
            fields
                .iter()
                .find_map(|(candidate, value)| (candidate == name).then_some(value))
        };
        let parsed = (|| {
            if fields.len() != 3 {
                return None;
            }
            let owner = decode_event_bytes(field("owner")?)?.try_into().ok()?;
            let delegate = decode_event_bytes(field("delegate")?)?.try_into().ok()?;
            let mode = match decode_unit_variant(
                field("new_mode")?,
                &[
                    "PlaceOrCancelOrder",
                    "DepositOrWithdraw",
                    "UpdateSubaccount",
                    "Disable",
                ],
            )? {
                "PlaceOrCancelOrder" => DeepXDelegateMode::PlaceOrCancelOrder,
                "DepositOrWithdraw" => DeepXDelegateMode::DepositOrWithdraw,
                "UpdateSubaccount" => DeepXDelegateMode::UpdateSubaccount,
                "Disable" => DeepXDelegateMode::Disable,
                _ => return None,
            };
            Some((owner, delegate, mode))
        })()
        .ok_or(DeepXDelegateModeEventError::InvalidChange(event_index))?;
        changes.push(DeepXFinalizedDelegateModeChange {
            block_hash,
            block_number,
            event_index,
            extrinsic_index,
            owner: parsed.0,
            delegate: parsed.1,
            mode: parsed.2,
        });
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(DeepXDelegateModeEventError::MalformedEventBytes);
    }
    Ok(changes)
}

/// Binds a mode change and successful dispatch to one finalized extrinsic location.
///
/// The caller supplies complete `System.Events` read at `location.block_hash()` and independently
/// verifies that the located signed extrinsic encodes the expected mode-change call. This function
/// does not authorize submission or treat a later REST delegate state as finalized.
///
/// # Errors
///
/// Returns an error for incomplete events, missing or duplicate same-index evidence, failed
/// dispatch, or a mode change that differs from the expected terms.
pub fn verify_finalized_delegate_mode_change_events(
    snapshot: &RuntimeSnapshot,
    location: DeepXFinalizedTransactionLocation,
    event_bytes: &[u8],
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_mode: DeepXDelegateMode,
) -> Result<DeepXFinalizedDelegateModeChange, DeepXDelegateModeVerificationError> {
    verify_finalized_delegate_mode_change_events_with_runtime(
        snapshot,
        location,
        event_bytes,
        expected_owner,
        expected_delegate,
        expected_mode,
    )
}

fn verify_finalized_delegate_mode_change_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    location: DeepXFinalizedTransactionLocation,
    event_bytes: &[u8],
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_mode: DeepXDelegateMode,
) -> Result<DeepXFinalizedDelegateModeChange, DeepXDelegateModeVerificationError> {
    snapshot.interfaces().event("System", "ExtrinsicSuccess")?;
    snapshot.interfaces().event("System", "ExtrinsicFailed")?;
    let changes = decode_finalized_delegate_mode_changes_with_runtime(
        snapshot,
        location.block_hash(),
        location.block_number(),
        event_bytes,
    )?;
    let mut change = None;
    for candidate in changes {
        if candidate.extrinsic_index != location.extrinsic_index() {
            continue;
        }
        if candidate.owner != expected_owner
            || candidate.delegate != expected_delegate
            || candidate.mode != expected_mode
        {
            return Err(DeepXDelegateModeVerificationError::ConflictingChange);
        }
        if change.replace(candidate).is_some() {
            return Err(DeepXDelegateModeVerificationError::Change);
        }
    }
    let mut dispatch = None;
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    for event in events.iter() {
        let event = event.map_err(|e| DeepXDelegateModeVerificationError::Decode(Box::new(e)))?;
        if event.phase() != Phase::ApplyExtrinsic(location.extrinsic_index())
            || event.pallet_name() != "System"
        {
            continue;
        }
        let outcome = match event.variant_name() {
            "ExtrinsicSuccess" => DeepXDispatchOutcome::Success,
            "ExtrinsicFailed" => DeepXDispatchOutcome::Failed,
            _ => continue,
        };
        if dispatch.replace(outcome).is_some() {
            return Err(DeepXDelegateModeVerificationError::Dispatch);
        }
    }
    if dispatch != Some(DeepXDispatchOutcome::Success) {
        return Err(DeepXDelegateModeVerificationError::Dispatch);
    }
    change.ok_or(DeepXDelegateModeVerificationError::Change)
}

/// Failures observing a delegate change at an exact finalized location.
#[derive(Debug, Error)]
pub enum DeepXFinalizedDelegateObservationError {
    /// Watch endpoint capabilities are absent or belong to a different endpoint.
    #[error("DeepX Watch RPC capabilities do not authorize delegate reads")]
    CapabilitiesMismatch,
    /// The supplied location is above the observed finalized head.
    #[error("DeepX delegate block {requested} is above finalized block {finalized}")]
    NotFinalized { requested: u64, finalized: u64 },
    /// The historical runtime request or validation failed.
    #[error("failed to observe DeepX historical delegate runtime: {0}")]
    HistoricalRuntime(#[from] DeepXHistoricalRuntimeError),
    /// Canonical block or event-storage observation failed.
    #[error("failed to observe DeepX delegate block: {0}")]
    Watch(#[from] DeepXTransactionWatchError),
    /// Historical metadata or runtime version is not fixture-approved.
    #[error("DeepX historical delegate runtime is not approved: {0}")]
    Runtime(#[from] SnapshotError),
    /// Historical metadata is not a complete prefixed SCALE value.
    #[error("invalid DeepX historical delegate metadata")]
    InvalidMetadata,
    /// The signed call used a different approved runtime than the event block.
    #[error("DeepX delegate signing runtime differs from the historical block runtime")]
    RuntimeMismatch,
    /// Event storage changed between reads at the same block hash.
    #[error("DeepX delegate event storage changed at a fixed block hash")]
    EventStorageChanged,
    /// Canonical block identity or target membership changed during observation.
    #[error("DeepX finalized delegate location changed during observation")]
    LocationChanged,
    /// Complete events do not prove the expected successful change.
    #[error(transparent)]
    Verification(#[from] DeepXDelegateModeVerificationError),
    /// Complete events do not prove the expected account setup or removal.
    #[error(transparent)]
    AccountVerification(#[from] DeepXDelegateAccountVerificationError),
}

/// Observes one delegate mode change from the exact finalized Watch block.
///
/// Historical metadata must match the configured testnet genesis. The target extrinsic membership
/// and complete event storage are each checked twice. The caller must independently prove that the
/// target signed extrinsic encodes the expected wallet call; this does not submit or activate it.
///
/// # Errors
///
/// Returns an error for non-finality, invalid runtime, changed block or event evidence, or
/// failed, missing, duplicate, or conflicting mode-change evidence.
pub async fn observe_finalized_delegate_mode_change_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    location: DeepXFinalizedTransactionLocation,
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_mode: DeepXDelegateMode,
) -> Result<DeepXFinalizedDelegateModeChange, DeepXFinalizedDelegateObservationError> {
    observe_finalized_delegate_mode_change_at_inner(
        environment,
        endpoints,
        capabilities,
        location,
        ExpectedDelegateModeChange {
            owner: expected_owner,
            delegate: expected_delegate,
            mode: expected_mode,
        },
        None,
    )
    .await
}

#[derive(Clone, Copy)]
struct ExpectedDelegateModeChange {
    owner: [u8; 20],
    delegate: [u8; 20],
    mode: DeepXDelegateMode,
}

async fn observe_finalized_delegate_mode_change_at_inner(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    location: DeepXFinalizedTransactionLocation,
    expected: ExpectedDelegateModeChange,
    expected_runtime: Option<&ApprovedRuntimeIdentity>,
) -> Result<DeepXFinalizedDelegateModeChange, DeepXFinalizedDelegateObservationError> {
    observe_finalized_delegate_change_at_inner(
        environment,
        endpoints,
        capabilities,
        location,
        expected_runtime,
        |snapshot, events| {
            verify_finalized_delegate_mode_change_events_with_runtime(
                snapshot,
                location,
                events,
                expected.owner,
                expected.delegate,
                expected.mode,
            )
            .map_err(DeepXFinalizedDelegateObservationError::Verification)
        },
    )
    .await
}

/// Observes delegate setup or removal from the exact finalized Watch block.
///
/// The runtime must be fixture-approved, and target membership and complete event storage are
/// checked twice. The caller separately binds the located signed call, including setup label and
/// expiry, which are absent from the runtime event. This does not enable management submission.
///
/// # Errors
///
/// Returns an error for non-finality, invalid runtime, changed block or event evidence, or
/// missing, duplicate, failed, or conflicting account-change evidence.
pub async fn observe_finalized_delegate_account_change_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    location: DeepXFinalizedTransactionLocation,
    expected_owner: [u8; 20],
    expected_delegate: [u8; 20],
    expected_kind: DeepXDelegateAccountChangeKind,
) -> Result<DeepXFinalizedDelegateAccountChange, DeepXFinalizedDelegateObservationError> {
    observe_finalized_delegate_change_at_inner(
        environment,
        endpoints,
        capabilities,
        location,
        None,
        |snapshot, events| {
            verify_finalized_delegate_account_change_events_with_runtime(
                snapshot,
                location,
                events,
                expected_owner,
                expected_delegate,
                expected_kind,
            )
            .map_err(DeepXFinalizedDelegateObservationError::AccountVerification)
        },
    )
    .await
}

/// Exact offline wallet delegate call to locate and verify without submitting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeepXSignedDelegateAccountOperation {
    /// Delegate creation or renewal, including the signed label and expiry.
    Set(DeepXSetDelegateAccountParams),
    /// Delegate removal.
    Remove(DeepXRemoveDelegateAccountParams),
}

/// Exact signing terms and requested height for one delegate account observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXSignedDelegateAccountQuery {
    /// Typed runtime call to regenerate exactly.
    pub operation: DeepXSignedDelegateAccountOperation,
    /// Explicit account nonce encoded in the signed extrinsic.
    pub nonce: u64,
    /// Block height at which the exact signed extrinsic is expected.
    pub block_number: u64,
}

/// Result of observing one exact offline-signed delegate account call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeepXSignedDelegateAccountObservation {
    /// The Watch finalized head has not reached the requested block.
    Pending(DeepXFinalizedRecoveryCheckpoint),
    /// The exact signed call and matching event succeeded in the finalized block.
    Finalized(DeepXFinalizedDelegateAccountChange),
}

/// Failures binding an offline-signed delegate account call to finalized chain evidence.
#[derive(Debug, Error)]
pub enum DeepXSignedDelegateAccountObservationError {
    /// The permitted snapshot or signing key cannot produce the expected call.
    #[error(transparent)]
    Signing(#[from] SigningError),
    /// The exact signed extrinsic could not be located at the requested height.
    #[error(transparent)]
    Watch(#[from] DeepXTransactionWatchError),
    /// The located call lacks approved, matching runtime event evidence.
    #[error(transparent)]
    Events(#[from] DeepXFinalizedDelegateObservationError),
}

/// Observes one caller-owned, offline-signed delegate setup or removal without submitting it.
///
/// The exact signed bytes are regenerated from the retained permit, key, operation, and nonce.
/// Their hash must be present at the requested finalized height, and an approved historical
/// runtime must emit the matching account event and successful dispatch. The setup event does not
/// echo the signed label or expiry, and historical success does not prove current permissions.
///
/// # Errors
///
/// Returns an error if signing, finalized membership, historical runtime, or event evidence fails.
pub async fn observe_signed_delegate_account_change_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    query: &DeepXSignedDelegateAccountQuery,
) -> Result<DeepXSignedDelegateAccountObservation, DeepXSignedDelegateAccountObservationError> {
    let (signed, delegate, kind) = match &query.operation {
        DeepXSignedDelegateAccountOperation::Set(params) => (
            sign_set_delegate_account(permit, key, params.clone(), query.nonce)?,
            params.delegate,
            DeepXDelegateAccountChangeKind::Set,
        ),
        DeepXSignedDelegateAccountOperation::Remove(params) => (
            sign_remove_delegate_account(permit, key, *params, query.nonce)?,
            params.delegate,
            DeepXDelegateAccountChangeKind::Removed,
        ),
    };
    let location = observe_finalized_transaction_location(
        endpoints,
        capabilities,
        signed.extrinsic_hash(),
        query.block_number,
    )
    .await?;
    let location = match location {
        DeepXFinalizedTransactionLocationObservation::Pending(checkpoint) => {
            return Ok(DeepXSignedDelegateAccountObservation::Pending(checkpoint));
        }
        DeepXFinalizedTransactionLocationObservation::Finalized(location) => location,
    };
    let change = observe_finalized_delegate_change_at_inner(
        environment,
        endpoints,
        capabilities,
        location,
        Some(signed.runtime()),
        |snapshot, events| {
            verify_finalized_delegate_account_change_events_with_runtime(
                snapshot,
                location,
                events,
                signed.signer(),
                delegate,
                kind,
            )
            .map_err(DeepXFinalizedDelegateObservationError::AccountVerification)
        },
    )
    .await?;
    Ok(DeepXSignedDelegateAccountObservation::Finalized(change))
}

async fn observe_finalized_delegate_change_at_inner<T>(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    location: DeepXFinalizedTransactionLocation,
    expected_runtime: Option<&ApprovedRuntimeIdentity>,
    verify: impl FnOnce(
        &DeepXReadOnlyRuntimeSnapshot,
        &[u8],
    ) -> Result<T, DeepXFinalizedDelegateObservationError>,
) -> Result<T, DeepXFinalizedDelegateObservationError> {
    use DeepXFinalizedDelegateObservationError as Error;

    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != watch_url
        || DEEPX_WATCH_RPC_METHODS
            .iter()
            .any(|method| !watch_capabilities.methods().contains(*method))
    {
        return Err(Error::CapabilitiesMismatch);
    }
    let finalized = observe_finalized_checkpoint_at(watch_url).await?;
    if location.block_number() > finalized.block_number() {
        return Err(Error::NotFinalized {
            requested: location.block_number(),
            finalized: finalized.block_number(),
        });
    }
    let observe_location = || async {
        observe_canonical_block_at(
            watch_url,
            location.block_number(),
            location.extrinsic_hash(),
        )
        .await
    };
    let matches_location = |observation: DeepXCanonicalBlockObservation| {
        observation.block_hash() == location.block_hash()
            && observation.extrinsic_index() == Some(location.extrinsic_index())
    };
    if !matches_location(observe_location().await?) {
        return Err(Error::LocationChanged);
    }

    let snapshot = observe_historical_read_only_runtime(
        environment,
        endpoints.genesis_hash(),
        watch_url,
        location.block_hash(),
    )
    .await?;
    if expected_runtime.is_some_and(|runtime| runtime != snapshot.identity()) {
        return Err(Error::RuntimeMismatch);
    }
    let first =
        fetch_system_events(watch_url, location.block_hash(), location.block_number()).await?;
    let second =
        fetch_system_events(watch_url, location.block_hash(), location.block_number()).await?;
    if first != second {
        return Err(Error::EventStorageChanged);
    }
    let change = verify(&snapshot, &first)?;
    if !matches_location(observe_location().await?) {
        return Err(Error::LocationChanged);
    }
    Ok(change)
}

/// Result of observing one exact offline-signed delegate mode call at a requested height.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeepXSignedDelegateModeObservation {
    /// The Watch finalized head has not reached the requested block.
    Pending(DeepXFinalizedRecoveryCheckpoint),
    /// The exact signed call and matching mode change succeeded in the finalized block.
    Finalized(DeepXFinalizedDelegateModeChange),
}

/// Failures binding an offline-signed delegate mode call to finalized chain evidence.
#[derive(Debug, Error)]
pub enum DeepXSignedDelegateModeObservationError {
    /// The caller's snapshot permit or signing key cannot produce the exact expected call.
    #[error(transparent)]
    Signing(#[from] SigningError),
    /// The exact signed extrinsic could not be located at the requested height.
    #[error(transparent)]
    Watch(#[from] DeepXTransactionWatchError),
    /// The located call lacks approved, matching runtime event evidence.
    #[error(transparent)]
    Events(#[from] DeepXFinalizedDelegateObservationError),
}

/// Exact offline signing terms and requested chain height for one read-only observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeepXSignedDelegateModeQuery {
    /// Delegate address and writable runtime mode.
    pub params: DeepXUpdateDelegateModeParams,
    /// Explicit account nonce encoded in the signed extrinsic.
    pub nonce: u64,
    /// Block height at which the exact signed extrinsic is expected.
    pub block_number: u64,
}

/// Observes one caller-owned, offline-signed delegate mode update without submitting it.
///
/// The exact signed bytes are regenerated with the supplied permit, key, arguments, and nonce.
/// Their hash must be present in the finalized canonical block at `block_number`; the event and
/// successful dispatch must then match the signer and requested terms. This does not authorize a
/// management transaction or infer current delegate permissions from historical success.
///
/// # Errors
///
/// Returns an error if signing, finalized membership, approved finalized runtime selection, or
/// same-index event evidence fails.
pub async fn observe_signed_delegate_mode_change_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    permit: &DeepXRuntimeSnapshotPermit,
    key: &DeepXPrivateKey,
    query: DeepXSignedDelegateModeQuery,
) -> Result<DeepXSignedDelegateModeObservation, DeepXSignedDelegateModeObservationError> {
    let signed = sign_update_delegate_mode(permit, key, query.params, query.nonce)?;
    let location = observe_finalized_transaction_location(
        endpoints,
        capabilities,
        signed.extrinsic_hash(),
        query.block_number,
    )
    .await?;
    let location = match location {
        DeepXFinalizedTransactionLocationObservation::Pending(checkpoint) => {
            return Ok(DeepXSignedDelegateModeObservation::Pending(checkpoint));
        }
        DeepXFinalizedTransactionLocationObservation::Finalized(location) => location,
    };
    let mode = match query.params.new_mode {
        DeepXWritableDelegateMode::PlaceOrCancelOrder => DeepXDelegateMode::PlaceOrCancelOrder,
        DeepXWritableDelegateMode::Disable => DeepXDelegateMode::Disable,
    };
    let change = observe_finalized_delegate_mode_change_at_inner(
        environment,
        endpoints,
        capabilities,
        location,
        ExpectedDelegateModeChange {
            owner: signed.signer(),
            delegate: query.params.address,
            mode,
        },
        Some(signed.runtime()),
    )
    .await?;
    Ok(DeepXSignedDelegateModeObservation::Finalized(change))
}

/// Decodes perpetual trades from one complete finalized `System.Events` value.
///
/// Event index and block identity are retained without assuming how REST trade IDs are formed.
/// The caller must establish canonical finality for the supplied block hash separately.
///
/// # Errors
///
/// Returns an error for an unsupported runtime event, incomplete SCALE value, or invalid trade.
pub fn decode_finalized_perp_trade_events(
    snapshot: &RuntimeSnapshot,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedPerpTrade>, DeepXPerpTradeEventError> {
    decode_finalized_perp_trade_events_with_metadata(
        snapshot.interfaces(),
        snapshot.metadata(),
        block_hash,
        block_number,
        event_bytes,
    )
}

fn decode_finalized_perp_trade_events_with_metadata(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &subxt_core::metadata::Metadata,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedPerpTrade>, DeepXPerpTradeEventError> {
    interfaces.event("PerpMarket", "NewTrade")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXPerpTradeEventError::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut trades = Vec::new();
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    for event in events.iter() {
        let event = event.map_err(|e| DeepXPerpTradeEventError::Decode(Box::new(e)))?;
        let event_index = count;
        count = count
            .checked_add(1)
            .ok_or(DeepXPerpTradeEventError::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(DeepXPerpTradeEventError::MalformedEventBytes)?;
        if event.pallet_name() != "PerpMarket" || event.variant_name() != "NewTrade" {
            continue;
        }
        let fields = event
            .field_values()
            .map_err(|e| DeepXPerpTradeEventError::Decode(Box::new(e)))?;
        let trade =
            parse_finalized_perp_trade_fields(&fields, block_hash, block_number, event_index)
                .ok_or(DeepXPerpTradeEventError::InvalidTrade(event_index))?;
        trades.push(trade);
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(DeepXPerpTradeEventError::MalformedEventBytes);
    }
    Ok(trades)
}

/// Decodes Spot matches from one complete finalized `System.Events` value.
///
/// The caller independently establishes canonical finality for the supplied block identity.
/// Event indices are global positions within the complete event sequence.
///
/// # Errors
///
/// Returns an error for unsupported metadata, incomplete SCALE bytes, or invalid trade fields.
pub fn decode_finalized_spot_trade_events(
    snapshot: &RuntimeSnapshot,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedSpotTrade>, DeepXSpotTradeEventError> {
    decode_finalized_spot_trade_events_with_metadata(
        snapshot.interfaces(),
        snapshot.metadata(),
        block_hash,
        block_number,
        event_bytes,
    )
}

fn decode_finalized_spot_trade_events_with_metadata(
    interfaces: &DeepXRuntimeInterfaceCatalog,
    metadata: &subxt_core::metadata::Metadata,
    block_hash: [u8; 32],
    block_number: u64,
    event_bytes: &[u8],
) -> Result<Vec<DeepXFinalizedSpotTrade>, DeepXSpotTradeEventError> {
    interfaces.event("SpotMarket", "OrderMatched")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXSpotTradeEventError::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut trades = Vec::new();
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    for event in events.iter() {
        let event = event.map_err(|e| DeepXSpotTradeEventError::Decode(Box::new(e)))?;
        let event_index = count;
        count = count
            .checked_add(1)
            .ok_or(DeepXSpotTradeEventError::MalformedEventBytes)?;
        consumed = consumed
            .checked_add(event.bytes().len())
            .ok_or(DeepXSpotTradeEventError::MalformedEventBytes)?;
        if event.pallet_name() != "SpotMarket" || event.variant_name() != "OrderMatched" {
            continue;
        }
        let fields = event
            .field_values()
            .map_err(|e| DeepXSpotTradeEventError::Decode(Box::new(e)))?;
        let trade =
            parse_finalized_spot_trade_fields(&fields, block_hash, block_number, event_index)
                .ok_or(DeepXSpotTradeEventError::InvalidTrade(event_index))?;
        trades.push(trade);
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(DeepXSpotTradeEventError::MalformedEventBytes);
    }
    Ok(trades)
}

/// Failures observing historical perpetual executions at a finalized block.
#[derive(Debug, Error)]
pub enum DeepXFinalizedPerpTradeObservationError {
    /// Watch endpoint capabilities are absent or belong to a different endpoint.
    #[error("DeepX Watch RPC capabilities do not authorize finalized trade reads")]
    CapabilitiesMismatch,
    /// The requested height is above the observed finalized head.
    #[error("DeepX trade block {requested} is above finalized block {finalized}")]
    NotFinalized { requested: u64, finalized: u64 },
    /// The historical runtime request or validation failed.
    #[error("failed to observe DeepX historical trade runtime: {0}")]
    HistoricalRuntime(#[from] DeepXHistoricalRuntimeError),
    /// Canonical block or event-storage observation failed.
    #[error("failed to observe DeepX finalized trade block: {0}")]
    Watch(#[from] DeepXTransactionWatchError),
    /// Historical metadata or runtime version could not be trusted.
    #[error("DeepX historical trade runtime is not approved: {0}")]
    Runtime(#[from] SnapshotError),
    /// Historical metadata is not a complete prefixed SCALE value.
    #[error("invalid DeepX historical trade metadata")]
    InvalidMetadata,
    /// One historical event read disagreed with another at the same block hash.
    #[error("DeepX historical trade event storage changed at a fixed block hash")]
    EventStorageChanged,
    /// The target block hash changed while reading its events.
    #[error("DeepX historical trade block hash changed during observation")]
    CanonicalHashChanged,
    /// Complete events could not be decoded against the approved runtime.
    #[error("DeepX finalized trade event decoding failed: {0}")]
    Events(#[from] DeepXPerpTradeEventError),
}

/// Observes all perpetual trades in one finalized historical block through the Watch endpoint.
///
/// The runtime metadata must match an approved fixture for the selected hash. Complete event
/// storage is read twice, and canonical block identity is rechecked before returning any trade.
/// These checks cannot replace a storage proof from the endpoint.
///
/// # Errors
///
/// Returns an error for unsupported capabilities or runtime, non-finality, inconsistent block or
/// event bytes, malformed storage, or invalid trade events.
pub async fn observe_finalized_perp_trades_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    block_number: u64,
) -> Result<Vec<DeepXFinalizedPerpTrade>, DeepXFinalizedPerpTradeObservationError> {
    use DeepXFinalizedPerpTradeObservationError as Error;

    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != watch_url
        || DEEPX_WATCH_RPC_METHODS
            .iter()
            .any(|method| !watch_capabilities.methods().contains(*method))
    {
        return Err(Error::CapabilitiesMismatch);
    }
    let finalized = observe_finalized_checkpoint_at(watch_url).await?;
    if block_number > finalized.block_number() {
        return Err(Error::NotFinalized {
            requested: block_number,
            finalized: finalized.block_number(),
        });
    }
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    let read_hash = || async {
        let encoded: Option<String> = client
            .execute_rpc_call(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "chain_getBlockHash",
                "params": [block_number],
            }))
            .await
            .map_err(|source| DeepXTransactionWatchError::Rpc {
                method: "chain_getBlockHash",
                source,
            })?;
        decode_hash(&encoded.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?)
    };
    let block_hash = read_hash().await?;
    let encoded_hash = format!("0x{}", hex::encode(block_hash));
    let header: Option<RpcBlockHeader> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "chain_getHeader", "params": [encoded_hash],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getHeader",
            source,
        })?;
    let header = header.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?;
    let received = decode_block_number(&header.number)?;
    if received != block_number {
        return Err(DeepXTransactionWatchError::BlockNumberMismatch {
            requested: block_number,
            received,
        }
        .into());
    }
    let snapshot = observe_historical_read_only_runtime(
        environment,
        endpoints.genesis_hash(),
        watch_url,
        block_hash,
    )
    .await?;
    let first = fetch_system_events(watch_url, block_hash, block_number).await?;
    let second = fetch_system_events(watch_url, block_hash, block_number).await?;
    if first != second {
        return Err(Error::EventStorageChanged);
    }
    let trades = decode_finalized_perp_trade_events_with_metadata(
        snapshot.interfaces(),
        snapshot.metadata(),
        block_hash,
        block_number,
        &first,
    )?;
    if read_hash().await? != block_hash {
        return Err(Error::CanonicalHashChanged);
    }
    Ok(trades)
}

/// Failures observing Spot matches at one finalized historical block.
#[derive(Debug, Error)]
pub enum DeepXFinalizedSpotTradeObservationError {
    /// Watch endpoint capabilities are absent or belong to a different endpoint.
    #[error("DeepX Watch RPC capabilities do not authorize finalized Spot trade reads")]
    CapabilitiesMismatch,
    /// The requested height is above the observed finalized head.
    #[error("DeepX Spot trade block {requested} is above finalized block {finalized}")]
    NotFinalized { requested: u64, finalized: u64 },
    /// The historical runtime request or validation failed.
    #[error("failed to observe DeepX historical Spot trade runtime: {0}")]
    HistoricalRuntime(#[from] DeepXHistoricalRuntimeError),
    /// Canonical block or event storage observation failed.
    #[error("failed to observe DeepX finalized Spot trade block: {0}")]
    Watch(#[from] DeepXTransactionWatchError),
    /// Historical metadata or runtime version is not fixture-approved.
    #[error("DeepX historical Spot trade runtime is not approved: {0}")]
    Runtime(#[from] SnapshotError),
    /// Historical metadata was not a complete prefixed SCALE value.
    #[error("invalid DeepX historical Spot trade metadata")]
    InvalidMetadata,
    /// Repeated event storage reads at one hash disagreed.
    #[error("DeepX historical Spot trade event storage changed at a fixed block hash")]
    EventStorageChanged,
    /// The canonical block hash changed during the read.
    #[error("DeepX historical Spot trade block hash changed during observation")]
    CanonicalHashChanged,
    /// Complete events could not be decoded under approved metadata.
    #[error("DeepX finalized Spot trade event decoding failed: {0}")]
    Events(#[from] DeepXSpotTradeEventError),
}

/// Observes all Spot matches in one finalized historical block through the Watch endpoint.
///
/// Runtime identity must match an approved fixture at the block hash. Complete event storage is
/// read twice and canonical identity is rechecked before any trade is returned. These observations
/// do not replace a cryptographic storage proof.
///
/// # Errors
///
/// Returns an error for unsupported capabilities or runtime, non-finality, inconsistent block or
/// event storage, malformed bytes, or invalid Spot trade fields.
pub async fn observe_finalized_spot_trades_at(
    environment: &DeepXEnvironment,
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    block_number: u64,
) -> Result<Vec<DeepXFinalizedSpotTrade>, DeepXFinalizedSpotTradeObservationError> {
    use DeepXFinalizedSpotTradeObservationError as Error;

    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != watch_url
        || DEEPX_WATCH_RPC_METHODS
            .iter()
            .any(|method| !watch_capabilities.methods().contains(*method))
    {
        return Err(Error::CapabilitiesMismatch);
    }
    let finalized = observe_finalized_checkpoint_at(watch_url).await?;
    if block_number > finalized.block_number() {
        return Err(Error::NotFinalized {
            requested: block_number,
            finalized: finalized.block_number(),
        });
    }
    let client = BlockchainHttpRpcClient::new(watch_url.to_string(), None, None);
    let read_hash = || async {
        let encoded: Option<String> = client
            .execute_rpc_call(json!({
                "jsonrpc": "2.0", "id": 1, "method": "chain_getBlockHash",
                "params": [block_number],
            }))
            .await
            .map_err(|source| DeepXTransactionWatchError::Rpc {
                method: "chain_getBlockHash",
                source,
            })?;
        decode_hash(&encoded.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?)
    };
    let block_hash = read_hash().await?;
    let encoded_hash = format!("0x{}", hex::encode(block_hash));
    let header: Option<RpcBlockHeader> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0", "id": 1, "method": "chain_getHeader", "params": [encoded_hash],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getHeader",
            source,
        })?;
    let header = header.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?;
    let received = decode_block_number(&header.number)?;
    if received != block_number {
        return Err(DeepXTransactionWatchError::BlockNumberMismatch {
            requested: block_number,
            received,
        }
        .into());
    }
    let snapshot = observe_historical_read_only_runtime(
        environment,
        endpoints.genesis_hash(),
        watch_url,
        block_hash,
    )
    .await?;
    let first = fetch_system_events(watch_url, block_hash, block_number).await?;
    let second = fetch_system_events(watch_url, block_hash, block_number).await?;
    if first != second {
        return Err(Error::EventStorageChanged);
    }
    let trades = decode_finalized_spot_trade_events_with_metadata(
        snapshot.interfaces(),
        snapshot.metadata(),
        block_hash,
        block_number,
        &first,
    )?;
    if read_hash().await? != block_hash {
        return Err(Error::CanonicalHashChanged);
    }
    Ok(trades)
}

fn parse_finalized_perp_trade_fields(
    fields: &Composite<u32>,
    block_hash: [u8; 32],
    block_number: u64,
    event_index: u32,
) -> Option<DeepXFinalizedPerpTrade> {
    let Composite::Named(fields) = fields else {
        return None;
    };
    if fields.len() != 6 {
        return None;
    }
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    let ScaleValue {
        value: ValueDef::Composite(Composite::Named(trade)),
        ..
    } = field("trade")?
    else {
        return None;
    };
    if trade.len() != 11 {
        return None;
    }
    let trade_field = |name| {
        trade
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    let market_id = u16::try_from(trade_field("market_id")?.as_u128()?).ok()?;
    let buyer_order_id = u64::try_from(trade_field("buyer_order_id")?.as_u128()?).ok()?;
    let seller_order_id = u64::try_from(trade_field("seller_order_id")?.as_u128()?).ok()?;
    let buyer = decode_event_bytes(trade_field("buyer")?)?.try_into().ok()?;
    let seller = decode_event_bytes(trade_field("seller")?)?
        .try_into()
        .ok()?;
    let price = trade_field("price")?.as_u128()?;
    let size = trade_field("size")?.as_u128()?;
    let buyer_leverage = u64::try_from(trade_field("buyer_leverage")?.as_u128()?).ok()?;
    let seller_leverage = u64::try_from(trade_field("seller_leverage")?.as_u128()?).ok()?;
    let created_at = u64::try_from(trade_field("created_at")?.as_u128()?).ok()?;
    let filled_direction =
        decode_unit_variant(trade_field("filled_direction")?, &["Long", "Short", "Both"])?;
    let taker = decode_unit_variant(field("taker")?, &["Buyer", "Seller"])?;
    let taker_fee = field("taker_fee")?.as_i128()?;
    let maker_fee = field("maker_fee")?.as_i128()?;
    let fee_asset = decode_event_bytes(field("taker_fee_asset")?)?;
    if market_id == 0
        || price == 0
        || size == 0
        || buyer_leverage == 0
        || seller_leverage == 0
        || created_at != block_number
        || fee_asset.is_empty()
        || decode_event_bytes(field("maker_fee_asset")?)? != fee_asset
    {
        return None;
    }
    Some(DeepXFinalizedPerpTrade {
        block_hash,
        block_number,
        event_index,
        market_id,
        buyer_order_id,
        buyer,
        seller_order_id,
        seller,
        price,
        size,
        buyer_leverage,
        seller_leverage,
        filled_direction: filled_direction.to_string(),
        taker: taker.to_string(),
        taker_fee,
        maker_fee,
        fee_asset,
    })
}

fn parse_finalized_spot_trade_fields(
    fields: &Composite<u32>,
    block_hash: [u8; 32],
    block_number: u64,
    event_index: u32,
) -> Option<DeepXFinalizedSpotTrade> {
    let Composite::Named(fields) = fields else {
        return None;
    };
    if fields.len() != 13 {
        return None;
    }
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    let pair = decode_event_bytes(field("pair")?)?.try_into().ok()?;
    let buyer = decode_event_bytes(field("buyer")?)?.try_into().ok()?;
    let seller = decode_event_bytes(field("seller")?)?.try_into().ok()?;
    let buy_id = u64::try_from(field("buy_id")?.as_u128()?).ok()?;
    let sell_id = u64::try_from(field("sell_id")?.as_u128()?).ok()?;
    let trade_amount_u = decode_event_u256_le(field("trade_amount_u")?)?;
    let trade_amount_b = decode_event_u256_le(field("trade_amount_b")?)?;
    let price = decode_event_u256_le(field("price")?)?;
    let taker = decode_unit_variant(field("taker")?, &["Buyer", "Seller"])?;
    let taker_fee = field("taker_fee")?.as_i128()?;
    let maker_fee = field("maker_fee")?.as_i128()?;
    let taker_fee_asset = decode_event_bytes(field("taker_fee_asset")?)?;
    let maker_fee_asset = decode_event_bytes(field("maker_fee_asset")?)?;
    if pair == [0; 32]
        || buyer == [0; 20]
        || seller == [0; 20]
        || trade_amount_u == [0; 32]
        || trade_amount_b == [0; 32]
        || price == [0; 32]
        || taker_fee_asset.is_empty()
        || maker_fee_asset.is_empty()
    {
        return None;
    }
    Some(DeepXFinalizedSpotTrade {
        block_hash,
        block_number,
        event_index,
        pair,
        buyer,
        buy_id,
        seller,
        sell_id,
        trade_amount_u,
        trade_amount_b,
        taker: taker.to_string(),
        taker_fee,
        taker_fee_asset,
        maker_fee,
        maker_fee_asset,
        price,
    })
}

fn decode_event_u256_le(value: &ScaleValue<u32>) -> Option<[u8; 32]> {
    let ValueDef::Composite(composite) = &value.value else {
        return None;
    };
    let values: Vec<_> = composite.values().collect();
    if values.len() == 1 {
        return decode_event_u256_le(values[0]);
    }
    if values.len() != 4 {
        return None;
    }
    let mut bytes = [0; 32];
    for (index, value) in values.into_iter().enumerate() {
        let limb = u64::try_from(value.as_u128()?).ok()?;
        bytes[index * 8..(index + 1) * 8].copy_from_slice(&limb.to_le_bytes());
    }
    Some(bytes)
}

fn decode_event_bytes(value: &ScaleValue<u32>) -> Option<Vec<u8>> {
    let ValueDef::Composite(bytes) = &value.value else {
        return None;
    };
    let values: Vec<_> = bytes.values().collect();
    if values.len() == 1 && values[0].as_u128().is_none() {
        return decode_event_bytes(values[0]);
    }
    values
        .into_iter()
        .map(|value| u8::try_from(value.as_u128()?).ok())
        .collect()
}

fn decode_unit_variant<'a>(value: &ScaleValue<u32>, allowed: &'a [&str]) -> Option<&'a str> {
    let ValueDef::Variant(variant) = &value.value else {
        return None;
    };
    allowed
        .iter()
        .copied()
        .find(|name| variant.name == *name && variant.values.values().next().is_none())
}

/// Failures while binding perpetual placement events to an approved durable operation.
#[derive(Debug, Error)]
pub enum DeepXPerpPlaceEventVerificationError {
    /// The approved runtime does not expose the required event interface.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The operation, nonce domain, or exact runtime scope cannot be verified.
    #[error("unsupported DeepX perpetual placement event identity")]
    UnsupportedOperation,
    /// SCALE event decoding failed against the approved metadata.
    #[error("unable to decode DeepX perpetual placement events: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// System.Events was truncated, count-mismatched, or contained trailing bytes.
    #[error("DeepX perpetual placement evidence is not complete System.Events")]
    MalformedEventBytes,
    /// The placement event disagrees with durable order terms.
    #[error("DeepX perpetual placement event conflicts with durable order terms")]
    ConflictingEvent,
    /// The target extrinsic emitted duplicate placement or dispatch events.
    #[error("duplicate DeepX perpetual placement evidence")]
    DuplicateEvent,
    /// Indexed dispatch and business evidence could not establish inclusion.
    #[error(transparent)]
    InclusionEvidence(#[from] DeepXInclusionEvidenceError),
}

/// Verifies complete perpetual placement dispatch and business events at one extrinsic index.
///
/// A successful dispatch requires exactly one matching `PerpMarket.OrderPlaced`. Both order IDs,
/// owner, market, side, size, order type, optional points and flags must match durable inputs.
/// Limit price must match; market price is runtime-computed and is not compared to the input.
/// This proves indexed event binding, not block canonicality, signer authorization, or fill success.
///
/// # Errors
///
/// Returns an error for unsupported scope, malformed SCALE, conflicting/duplicate events, or
/// successful dispatch without the expected placement event.
pub fn verify_perp_place_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpPlaceEventVerificationError> {
    verify_perp_place_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_perp_place_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpPlaceEventVerificationError> {
    use DeepXPerpPlaceEventVerificationError as Error;
    let Some(DeepXTransactionOperation::PerpPlace { is_long, .. }) = identity.operation() else {
        return Err(Error::UnsupportedOperation);
    };
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(Error::UnsupportedOperation);
    }
    let expected_side = if *is_long {
        nautilus_model::enums::OrderSide::Buy
    } else {
        nautilus_model::enums::OrderSide::Sell
    };
    if identity.order_side() != expected_side {
        return Err(Error::ConflictingEvent);
    }
    verify_indexed_perp_events(
        snapshot,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
        "OrderPlaced",
        |fields| perp_place_fields_match(fields, identity, block_number),
    )
}

/// Verifies TP/SL updates against exact durable points, including zero-to-`None` conversion.
///
/// Only operation-proven position fields are compared. This does not establish canonicality
/// or authorize submission. A successful dispatch requires one matching position update.
///
/// # Errors
///
/// Returns an error for foreign scope, incomplete events, conflicting fields or missing evidence.
pub fn verify_perp_profit_and_loss_point_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpProfitAndLossPointEventVerificationError> {
    verify_perp_profit_and_loss_point_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_perp_profit_and_loss_point_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpProfitAndLossPointEventVerificationError> {
    let Some(DeepXTransactionOperation::PerpProfitAndLossPoint {
        subaccount,
        market_id,
        take_profit_point,
        stop_loss_point,
    }) = identity.operation()
    else {
        return Err(DeepXPerpProfitAndLossPointEventVerificationError::UnsupportedOperation);
    };
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(DeepXPerpProfitAndLossPointEventVerificationError::UnsupportedOperation);
    }
    verify_indexed_perp_events(
        snapshot,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
        "PositionUpdated",
        |fields| {
            let Composite::Named(fields) = fields else {
                return false;
            };
            let field = |name| {
                fields
                    .iter()
                    .find_map(|(key, value)| (key == name).then_some(value))
            };
            if fields.len() != 4
                || !field("owner").is_some_and(|v| value_matches_bytes(v, subaccount))
                || field("market_id").and_then(ScaleValue::as_u128) != Some(u128::from(*market_id))
                || field("pnl").and_then(ScaleValue::as_i128) != Some(0)
            {
                return false;
            }
            let Some(ScaleValue {
                value: ValueDef::Composite(Composite::Named(pos)),
                ..
            }) = field("pos")
            else {
                return false;
            };
            let field = |name| {
                pos.iter()
                    .find_map(|(key, value)| (key == name).then_some(value))
            };
            field("owner").is_some_and(|v| value_matches_bytes(v, subaccount))
                && field("market_id").and_then(ScaleValue::as_u128) == Some(u128::from(*market_id))
                && field("take_profit").is_some_and(|v| {
                    value_matches_optional_u128(
                        v,
                        (*take_profit_point != 0).then_some(*take_profit_point),
                    )
                })
                && field("stop_loss").is_some_and(|v| {
                    value_matches_optional_u128(
                        v,
                        (*stop_loss_point != 0).then_some(*stop_loss_point),
                    )
                })
        },
    )
    .map_err(DeepXPerpProfitAndLossPointEventVerificationError::IndexedEvents)
}

/// Failures binding TP/SL position updates to a durable operation.
#[derive(Debug, Error)]
pub enum DeepXPerpProfitAndLossPointEventVerificationError {
    /// The exact runtime, operation or nonce domain is unsupported.
    #[error("unsupported DeepX perpetual TP/SL event identity")]
    UnsupportedOperation,
    /// Shared indexed perpetual dispatch/business event validation failed.
    #[error("DeepX perpetual TP/SL indexed event verification failed: {0}")]
    IndexedEvents(#[source] DeepXPerpPlaceEventVerificationError),
}

fn verify_indexed_perp_events(
    snapshot: &impl RuntimeMetadataView,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
    event_name: &str,
    fields_match: impl Fn(&Composite<u32>) -> bool,
) -> Result<DeepXInclusionEvidence, DeepXPerpPlaceEventVerificationError> {
    use DeepXPerpPlaceEventVerificationError as Error;
    snapshot.interfaces().event("PerpMarket", event_name)?;
    snapshot.interfaces().event("System", "ExtrinsicSuccess")?;
    snapshot.interfaces().event("System", "ExtrinsicFailed")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| Error::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut dispatch = None;
    let mut matched = false;
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    for event in events.iter() {
        let event = event.map_err(|e| Error::Decode(Box::new(e)))?;
        count += 1;
        consumed += event.bytes().len();
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index) {
            continue;
        }
        if event.pallet_name() == "System" {
            let outcome = match event.variant_name() {
                "ExtrinsicSuccess" => DeepXDispatchOutcome::Success,
                "ExtrinsicFailed" => DeepXDispatchOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "PerpMarket" && event.variant_name() == event_name {
            let fields = event
                .field_values()
                .map_err(|e| Error::Decode(Box::new(e)))?;
            if !fields_match(&fields) {
                return Err(Error::ConflictingEvent);
            }
            if matched {
                return Err(Error::DuplicateEvent);
            }
            matched = true;
        }
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(Error::MalformedEventBytes);
    }
    Ok(DeepXInclusionEvidence::from_indexed_observations(
        block_hash,
        block_number,
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: dispatch.ok_or(Error::MalformedEventBytes)?,
        },
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: if matched {
                DeepXBusinessEventOutcome::Success
            } else {
                DeepXBusinessEventOutcome::NotObserved
            },
        },
    )?)
}

fn perp_place_fields_match(
    fields: &Composite<u32>,
    identity: &DeepXTransactionIdentity,
    block_number: u64,
) -> bool {
    let Some(DeepXTransactionOperation::PerpPlace {
        subaccount,
        market_id,
        is_long,
        size,
        price,
        order_type,
        take_profit,
        stop_loss,
        reduce_only,
        post_only,
    }) = identity.operation()
    else {
        return false;
    };
    let super::DeepXNonceReservation::TimestampOrderId { value: order_id } = identity.nonce()
    else {
        return false;
    };
    let Composite::Named(fields) = fields else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    if fields.len() != 2
        || field("order_id").and_then(ScaleValue::as_u128) != Some(u128::from(order_id))
    {
        return false;
    }
    let Some(ScaleValue {
        value: ValueDef::Composite(Composite::Named(order)),
        ..
    }) = field("order")
    else {
        return false;
    };
    let field = |name| {
        order
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    let spot_order_type = match order_type {
        crate::signing::DeepXPerpOrderType::Limit(tif) => {
            crate::signing::DeepXSpotOrderType::Limit(*tif)
        }
        crate::signing::DeepXPerpOrderType::Market(slippage) => {
            crate::signing::DeepXSpotOrderType::Market(*slippage)
        }
        crate::signing::DeepXPerpOrderType::Stop => return false,
    };
    order.len() == 16
        && field("order_id").and_then(ScaleValue::as_u128) == Some(u128::from(order_id))
        && field("owner").is_some_and(|value| value_matches_bytes(value, subaccount))
        && field("market_id").and_then(ScaleValue::as_u128) == Some(u128::from(*market_id))
        && field("is_long").and_then(ScaleValue::as_bool) == Some(*is_long)
        && field("size").and_then(ScaleValue::as_u128) == Some(*size)
        && field("price").and_then(ScaleValue::as_u128).is_some_and(|actual| matches!(order_type, crate::signing::DeepXPerpOrderType::Market(_)) || actual == *price)
        && field("order_type").is_some_and(|value| value_matches_order_type(value, spot_order_type))
        && field("take_profit").is_some_and(|value| value_matches_optional_u128(value, *take_profit))
        && field("stop_loss").is_some_and(|value| value_matches_optional_u128(value, *stop_loss))
        && field("reduce_only").and_then(ScaleValue::as_bool) == Some(*reduce_only)
        && field("post_only").is_some_and(|value| value_matches_post_only(value, *post_only))
        && field("create_time").and_then(ScaleValue::as_u128) == Some(u128::from(block_number))
        && field("leverage").and_then(ScaleValue::as_u128).is_some()
        && field("status").is_some_and(|value| matches!(&value.value, ValueDef::Variant(variant) if variant.name == "Open" && variant.values.values().next().is_none()))
        && field("size_filled").and_then(ScaleValue::as_u128) == Some(0)
        && field("size_remain").and_then(ScaleValue::as_u128) == Some(*size)
}

fn value_matches_optional_u128(value: &ScaleValue<u32>, expected: Option<u128>) -> bool {
    let ValueDef::Variant(variant) = &value.value else {
        return false;
    };
    let values: Vec<_> = variant.values.values().collect();
    match expected {
        None => variant.name == "None" && values.is_empty(),
        Some(expected) => {
            variant.name == "Some" && values.len() == 1 && values[0].as_u128() == Some(expected)
        }
    }
}

/// Verifies the close-generated order without equating its account-sequence ID to the nonce.
///
/// This binds creation of a reduce-only order, not execution or complete position closure.
/// Position-derived direction and size are not inferred from the durable close call. The generated
/// order must still have positive size and leverage, with no initial fill.
///
/// # Errors
///
/// Returns an error for unsupported scope, malformed events or conflicting close-order terms.
pub fn verify_perp_close_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpCloseEventVerificationError> {
    verify_perp_close_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_perp_close_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpCloseEventVerificationError> {
    let Some(DeepXTransactionOperation::PerpClose {
        subaccount,
        market_id,
        price,
        slippage,
    }) = identity.operation()
    else {
        return Err(DeepXPerpCloseEventVerificationError::UnsupportedOperation);
    };
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(DeepXPerpCloseEventVerificationError::UnsupportedOperation);
    }
    verify_indexed_perp_events(snapshot, block_hash, block_number, extrinsic_index, event_bytes,
        "OrderPlaced", |fields| {
            let Composite::Named(outer) = fields else { return false; };
            let field = |name| outer.iter().find_map(|(key, value)| (key == name).then_some(value));
            let Some(order_id) = field("order_id").and_then(ScaleValue::as_u128).filter(|id| *id <= u128::from(u64::MAX)) else { return false; };
            let Some(ScaleValue { value: ValueDef::Composite(Composite::Named(order)), .. }) = field("order") else { return false; };
            let field = |name| order.iter().find_map(|(key, value)| (key == name).then_some(value));
            let Some(is_long) = field("is_long").and_then(ScaleValue::as_bool) else { return false; };
            let Some(size) = field("size").and_then(ScaleValue::as_u128).filter(|size| *size > 0) else { return false; };
            let Some(leverage) = field("leverage").and_then(ScaleValue::as_u128).filter(|leverage| *leverage > 0) else { return false; };
            outer.len() == 2 && order.len() == 16
                && field("order_id").and_then(ScaleValue::as_u128) == Some(order_id)
                && field("owner").is_some_and(|v| value_matches_bytes(v, subaccount))
                && field("market_id").and_then(ScaleValue::as_u128) == Some(u128::from(*market_id))
                && field("is_long").and_then(ScaleValue::as_bool) == Some(is_long)
                && field("size").and_then(ScaleValue::as_u128) == Some(size)
                && field("size_remain").and_then(ScaleValue::as_u128) == Some(size)
                && field("size_filled").and_then(ScaleValue::as_u128) == Some(0)
                && field("create_time").and_then(ScaleValue::as_u128) == Some(u128::from(block_number))
                && field("leverage").and_then(ScaleValue::as_u128) == Some(leverage)
                && field("price").and_then(ScaleValue::as_u128).is_some_and(|actual| *price == 0 || actual == *price)
                && field("order_type").is_some_and(|v| if *price == 0 {
                    value_matches_order_type(v, crate::signing::DeepXSpotOrderType::Market(*slippage))
                } else { matches!(&v.value, ValueDef::Variant(variant) if variant.name == "Stop" && variant.values.values().next().is_none()) })
                && field("status").is_some_and(|v| matches!(&v.value, ValueDef::Variant(variant) if variant.name == "Open" && variant.values.values().next().is_none()))
                && field("reduce_only").and_then(ScaleValue::as_bool) == Some(true)
                && field("post_only").is_some_and(|v| value_matches_post_only(v, crate::signing::DeepXPostOnlyParam::None))
                && field("take_profit").is_some_and(|v| value_matches_optional_u128(v, None))
                && field("stop_loss").is_some_and(|v| value_matches_optional_u128(v, None))
        }).map_err(DeepXPerpCloseEventVerificationError::IndexedEvents)
}

/// Failures binding a generated close order to durable call terms.
#[derive(Debug, Error)]
pub enum DeepXPerpCloseEventVerificationError {
    /// The exact operation, runtime or nonce domain is unsupported.
    #[error("unsupported DeepX perpetual close event identity")]
    UnsupportedOperation,
    /// Shared indexed perpetual event validation failed.
    #[error("DeepX perpetual close indexed event verification failed: {0}")]
    IndexedEvents(#[source] DeepXPerpPlaceEventVerificationError),
}

/// Errors raised while verifying offline Spot placement event evidence.
#[derive(Debug, Error)]
pub enum DeepXSpotPlaceEventVerificationError {
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    #[error("DeepX identity or runtime does not describe a supported Spot place")]
    UnsupportedOperation,
    #[error("unable to decode DeepX Spot place event evidence: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    #[error("DeepX Spot place event evidence is not a complete System.Events value")]
    MalformedEventBytes,
    #[error("DeepX Spot place event conflicts with the durable identity")]
    ConflictingEvent,
    #[error("DeepX Spot place evidence contains duplicate order state events")]
    DuplicateEvent,
    #[error(transparent)]
    InclusionEvidence(#[from] DeepXInclusionEvidenceError),
}

/// Verifies Spot placement inclusion against an approved offline snapshot.
///
/// Successful dispatch requires a unique, same-index `StateOrderBuy` or `StateOrderSell` event
/// whose durable input fields match the reservation. Failed dispatch is authoritative without a
/// business event.
///
/// # Errors
///
/// Returns an error for unsupported operations, conflicting identities, incomplete SCALE,
/// duplicate evidence, or successful dispatch without the expected business event.
pub fn verify_spot_place_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXSpotPlaceEventVerificationError> {
    verify_spot_place_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_spot_place_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXSpotPlaceEventVerificationError> {
    use DeepXSpotPlaceEventVerificationError as Error;
    let Some(DeepXTransactionOperation::SpotPlace {
        subaccount,
        pair,
        is_buy,
        quote_amount,
        base_amount,
        order_type,
        post_only,
        reduce_only,
    }) = identity.operation()
    else {
        return Err(Error::UnsupportedOperation);
    };
    let super::DeepXNonceReservation::TimestampOrderId { value: order_id } = identity.nonce()
    else {
        return Err(Error::UnsupportedOperation);
    };
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity()) {
        return Err(Error::UnsupportedOperation);
    }
    let expected_side = if *is_buy {
        nautilus_model::enums::OrderSide::Buy
    } else {
        nautilus_model::enums::OrderSide::Sell
    };
    if identity.order_side() != expected_side {
        return Err(Error::ConflictingEvent);
    }
    let event_name = if *is_buy {
        "StateOrderBuy"
    } else {
        "StateOrderSell"
    };
    snapshot.interfaces().event("SpotMarket", event_name)?;
    snapshot.interfaces().event("System", "ExtrinsicSuccess")?;
    snapshot.interfaces().event("System", "ExtrinsicFailed")?;

    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| Error::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut dispatch = None;
    let mut matched = false;
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    for event in events.iter() {
        let event = event.map_err(|e| Error::Decode(Box::new(e)))?;
        count += 1;
        consumed += event.bytes().len();
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index) {
            continue;
        }
        if event.pallet_name() == "System" {
            let outcome = match event.variant_name() {
                "ExtrinsicSuccess" => DeepXDispatchOutcome::Success,
                "ExtrinsicFailed" => DeepXDispatchOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "SpotMarket" && event.variant_name() == event_name {
            let fields = event
                .field_values()
                .map_err(|e| Error::Decode(Box::new(e)))?;
            let Composite::Named(fields) = fields else {
                return Err(Error::ConflictingEvent);
            };
            if fields.len() != 1
                || !fields.iter().any(|(name, order)| {
                    name == "order"
                        && spot_place_order_matches(
                            order,
                            *subaccount,
                            *pair,
                            order_id,
                            *quote_amount,
                            *base_amount,
                            *order_type,
                            *post_only,
                            *reduce_only,
                            *is_buy,
                        )
                })
            {
                return Err(Error::ConflictingEvent);
            }
            if matched {
                return Err(Error::DuplicateEvent);
            }
            matched = true;
        }
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(Error::MalformedEventBytes);
    }
    Ok(DeepXInclusionEvidence::from_indexed_observations(
        block_hash,
        block_number,
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: dispatch.ok_or(Error::MalformedEventBytes)?,
        },
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: if matched {
                DeepXBusinessEventOutcome::Success
            } else {
                DeepXBusinessEventOutcome::NotObserved
            },
        },
    )?)
}

/// Errors raised while verifying offline Spot cancellation event evidence.
#[derive(Debug, Error)]
pub enum DeepXSpotCancelEventVerificationError {
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    #[error("DeepX identity or runtime does not describe a supported Spot cancel")]
    UnsupportedOperation,
    #[error("DeepX fast Spot cancel event suppression is not approved")]
    FastCancelUnsupported,
    #[error("unable to decode DeepX Spot event evidence: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    #[error("DeepX Spot event evidence is not a complete System.Events value")]
    MalformedEventBytes,
    #[error("DeepX Spot cancel event conflicts with the durable identity")]
    ConflictingEvent,
    #[error("DeepX Spot cancel evidence contains duplicate events")]
    DuplicateEvent,
    #[error(transparent)]
    InclusionEvidence(#[from] DeepXInclusionEvidenceError),
}

/// Verifies ordinary Spot cancellation inclusion against an approved offline snapshot.
///
/// Successful dispatch requires a same-index, identity-matching `OrderCancelled` event.
/// Failed dispatch is authoritative without a successful business event. Fast cancellation
/// remains gated pending runtime-tagged evidence of event suppression.
///
/// # Errors
///
/// Returns an error for unsupported operations, conflicting identities, incomplete SCALE,
/// duplicate evidence, or successful dispatch without the expected business event.
pub fn verify_spot_cancel_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXSpotCancelEventVerificationError> {
    verify_spot_cancel_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_spot_cancel_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXSpotCancelEventVerificationError> {
    use DeepXSpotCancelEventVerificationError as Error;
    let Some(DeepXTransactionOperation::SpotCancel {
        subaccount,
        pair,
        order_id,
        is_buy,
        fast_cancel,
    }) = identity.operation()
    else {
        return Err(Error::UnsupportedOperation);
    };
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity()) {
        return Err(Error::UnsupportedOperation);
    }
    if *fast_cancel {
        return Err(Error::FastCancelUnsupported);
    }
    snapshot
        .interfaces()
        .event("SpotMarket", "OrderCancelled")?;
    snapshot.interfaces().event("System", "ExtrinsicSuccess")?;
    snapshot.interfaces().event("System", "ExtrinsicFailed")?;
    let mut remaining = event_bytes;
    let declared = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| Error::MalformedEventBytes)?
        .0;
    let mut consumed = event_bytes.len() - remaining.len();
    let mut count = 0_u32;
    let mut dispatch = None;
    let mut matched = false;
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    for event in events.iter() {
        let event = event.map_err(|e| Error::Decode(Box::new(e)))?;
        count += 1;
        consumed += event.bytes().len();
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index) {
            continue;
        }
        if event.pallet_name() == "System" {
            let outcome = match event.variant_name() {
                "ExtrinsicSuccess" => DeepXDispatchOutcome::Success,
                "ExtrinsicFailed" => DeepXDispatchOutcome::Failed,
                _ => continue,
            };
            if dispatch.replace(outcome).is_some() {
                return Err(Error::DuplicateEvent);
            }
        } else if event.pallet_name() == "SpotMarket" && event.variant_name() == "OrderCancelled" {
            let fields = event
                .field_values()
                .map_err(|e| Error::Decode(Box::new(e)))?;
            let Composite::Named(fields) = fields else {
                return Err(Error::ConflictingEvent);
            };
            let field = |name| {
                fields
                    .iter()
                    .find_map(|(candidate, value)| (candidate == name).then_some(value))
            };
            if fields.len() != 5
                || !field("pair").is_some_and(|value| value_matches_bytes(value, pair))
                || !field("maker").is_some_and(|value| value_matches_bytes(value, subaccount))
                || field("order_id").and_then(ScaleValue::as_u128) != Some(u128::from(*order_id))
                || field("is_buy").and_then(ScaleValue::as_bool) != Some(*is_buy)
                || !field("reason").is_some_and(|value| matches!(&value.value,
                    ValueDef::Variant(reason) if reason.name == "UserCanceled" && reason.values.values().next().is_none()))
            {
                return Err(Error::ConflictingEvent);
            }
            if matched {
                return Err(Error::DuplicateEvent);
            }
            matched = true;
        }
    }
    if count != declared || consumed != event_bytes.len() {
        return Err(Error::MalformedEventBytes);
    }
    Ok(DeepXInclusionEvidence::from_indexed_observations(
        block_hash,
        block_number,
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: dispatch.ok_or(Error::MalformedEventBytes)?,
        },
        DeepXIndexedOutcome {
            extrinsic_index,
            outcome: if matched {
                DeepXBusinessEventOutcome::Success
            } else {
                DeepXBusinessEventOutcome::NotObserved
            },
        },
    )?)
}

/// Errors raised while verifying a perpetual cancellation from runtime event evidence.
#[derive(Debug, Error)]
pub enum DeepXPerpCancelEventVerificationError {
    /// The approved runtime snapshot does not expose the required event.
    #[error(transparent)]
    RuntimeInterface(#[from] DeepXRuntimeInterfaceError),
    /// The durable identity does not describe an event-verifiable perpetual cancellation.
    #[error("DeepX transaction identity has no event-verifiable perpetual cancel operation")]
    UnsupportedOperation,
    /// Fast cancel deliberately omits the authoritative pallet cancellation event.
    #[error("DeepX fast perpetual cancel has no authoritative pallet event")]
    FastCancelUnsupported,
    /// Runtime event bytes could not be decoded against the approved snapshot.
    #[error("unable to decode DeepX runtime event evidence: {0}")]
    Decode(#[source] Box<subxt_core::Error>),
    /// Runtime event bytes were incomplete, count-mismatched, or contained trailing data.
    #[error("DeepX runtime event evidence is not a complete System.Events value")]
    MalformedEventBytes,
    /// A cancellation event at the target index did not match the durable operation.
    #[error("DeepX perpetual cancel event conflicts with the durable transaction identity")]
    ConflictingEvent,
    /// More than one matching cancellation event was observed at the target index.
    #[error("DeepX perpetual cancel emitted duplicate matching events")]
    DuplicateEvent,
    /// Indexed dispatch and business-event observations could not prove inclusion.
    #[error(transparent)]
    InclusionEvidence(#[from] DeepXInclusionEvidenceError),
}

/// Failures while verifying both legs of a perpetual modify at one extrinsic index.
#[derive(Debug, Error)]
pub enum DeepXPerpModifyEventVerificationError {
    /// The base identity, modify scope, or cancel mode cannot be verified.
    #[error("unsupported DeepX perpetual modify event identity")]
    UnsupportedOperation,
    /// The existing order cancellation event does not match the modify request.
    #[error(transparent)]
    Cancel(#[from] DeepXPerpCancelEventVerificationError),
    /// The replacement order placement event does not match the modify request.
    #[error(transparent)]
    Place(#[from] DeepXPerpPlaceEventVerificationError),
    /// Complete runtime events could not be decoded to verify leg ordering.
    #[error("DeepX perpetual modify event ordering could not be verified")]
    MalformedEventBytes,
    /// The cancel and replacement legs produced conflicting inclusion evidence.
    #[error("DeepX perpetual modify legs produced conflicting inclusion evidence")]
    ConflictingEvidence,
}

/// Verifies exact perpetual cancellation and replacement placement at the same extrinsic index.
///
/// The replacement order ID is the timestamp nonce. The identity must contain the exact modify
/// operation, reserved signer, instrument, side, nonce and runtime scope.
/// Both complete `System.Events` decodes must independently prove dispatch and the expected
/// business event. This verifies event evidence only; the signed call still requires canonical
/// `Subaccount.modify_orders` binding before it can be used for durable recovery.
///
/// # Errors
///
/// Returns an error for unsupported identity/scope, malformed event bytes, conflicting leg
/// evidence, or a successful dispatch missing either expected business event.
pub fn verify_perp_modify_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    params: DeepXPerpModifyParams,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpModifyEventVerificationError> {
    verify_perp_modify_inclusion_events_with_runtime(
        snapshot,
        identity,
        params,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_perp_modify_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    identity: &DeepXTransactionIdentity,
    params: DeepXPerpModifyParams,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpModifyEventVerificationError> {
    use DeepXPerpModifyEventVerificationError as Error;

    if !matches!(
        identity.nonce(),
        super::DeepXNonceReservation::TimestampOrderId { .. }
    ) {
        return Err(Error::UnsupportedOperation);
    }
    let expected_side = if params.place.is_long {
        nautilus_model::enums::OrderSide::Buy
    } else {
        nautilus_model::enums::OrderSide::Sell
    };
    let Some(DeepXTransactionOperation::PerpModify {
        subaccount,
        place_subaccount,
        cancel_order_id,
        market_id,
        place_market_id,
        fast_cancel,
        is_long,
        size,
        price,
        order_type,
        take_profit,
        stop_loss,
        reduce_only,
        post_only,
    }) = identity.operation()
    else {
        return Err(Error::UnsupportedOperation);
    };
    if *subaccount != params.cancel.subaccount
        || *place_subaccount != params.place.subaccount
        || *cancel_order_id != params.cancel.order_id
        || *market_id != params.cancel.market_id
        || *place_market_id != params.place.market_id
        || *fast_cancel != params.cancel.fast_cancel
        || *is_long != params.place.is_long
        || *size != params.place.size
        || *price != params.place.price
        || *order_type != params.place.order_type
        || *take_profit != params.place.take_profit
        || *stop_loss != params.place.stop_loss
        || *reduce_only != params.place.reduce_only
        || *post_only != params.place.post_only
        || identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || identity.order_side() != expected_side
        || params.cancel.fast_cancel
        || params.cancel.subaccount != params.place.subaccount
        || params.cancel.market_id != params.place.market_id
    {
        return Err(Error::UnsupportedOperation);
    }

    let client_order_id =
        nautilus_model::identifiers::ClientOrderId::new(identity.client_order_id());
    let cancel_identity = DeepXTransactionIdentity::new_perp_cancel(
        client_order_id,
        identity.signer(),
        identity.instrument_id(),
        expected_side,
        identity.nonce(),
        identity.runtime().clone(),
        params.cancel,
    );
    let cancel_evidence = verify_perp_cancel_inclusion_events_with_runtime(
        snapshot,
        &cancel_identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )?;
    let mut place_params = params.place;
    if cancel_evidence.outcome() == super::DeepXInclusionOutcome::Success {
        if !perp_modify_sizing_event_supported(snapshot.metadata()) {
            return Err(Error::UnsupportedOperation);
        }
        place_params.size = verify_perp_modify_sizing_event(
            snapshot.metadata(),
            identity,
            extrinsic_index,
            event_bytes,
        )?;
    }
    let place_identity = DeepXTransactionIdentity::new_perp_place(
        client_order_id,
        identity.signer(),
        identity.instrument_id(),
        expected_side,
        identity.nonce(),
        identity.runtime().clone(),
        place_params,
    );
    let place_evidence = verify_perp_place_inclusion_events_with_runtime(
        snapshot,
        &place_identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )?;
    if cancel_evidence != place_evidence {
        return Err(Error::ConflictingEvidence);
    }
    if cancel_evidence.outcome() == super::DeepXInclusionOutcome::Success
        && !perp_modify_events_are_ordered(snapshot, extrinsic_index, event_bytes)?
    {
        return Err(Error::ConflictingEvidence);
    }
    Ok(cancel_evidence)
}

pub(crate) fn perp_modify_sizing_event_supported(
    metadata: &subxt_core::metadata::Metadata,
) -> bool {
    let Some(event) = metadata
        .pallet_by_name("Subaccount")
        .and_then(|pallet| pallet.event_variants())
        .and_then(|variants| {
            variants
                .iter()
                .find(|variant| variant.name == "PerpOrderModified")
        })
    else {
        return false;
    };
    let expected = [
        "subaccount",
        "old_order_id",
        "filled_size",
        "target_size",
        "replacement_size",
        "new_order_id",
    ];
    event.fields.len() == expected.len()
        && event
            .fields
            .iter()
            .zip(expected)
            .all(|(field, name)| field.name.as_deref() == Some(name))
        && event
            .fields
            .iter()
            .enumerate()
            .skip(1)
            .all(|(index, field)| {
                let expected_type = if index == 1 || index == 5 {
                    TypeDefPrimitive::U64
                } else {
                    TypeDefPrimitive::U128
                };
                matches!(
                    metadata.types().resolve(field.ty.id).map(|ty| &ty.type_def),
                    Some(TypeDef::Primitive(primitive)) if *primitive == expected_type
                )
            })
}

fn verify_perp_modify_sizing_event(
    metadata: &subxt_core::metadata::Metadata,
    identity: &DeepXTransactionIdentity,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<u128, DeepXPerpModifyEventVerificationError> {
    use DeepXPerpModifyEventVerificationError as Error;

    let Some(DeepXTransactionOperation::PerpModify {
        subaccount,
        cancel_order_id,
        size: target_size,
        ..
    }) = identity.operation()
    else {
        return Err(Error::UnsupportedOperation);
    };
    let super::DeepXNonceReservation::TimestampOrderId {
        value: expected_new_order_id,
    } = identity.nonce()
    else {
        return Err(Error::UnsupportedOperation);
    };
    let events = Events::<DeepXRuntimeConfig>::decode_from(event_bytes.to_vec(), metadata.clone());
    let mut evidence = None;
    for event in events.iter() {
        let event = event.map_err(|_| Error::MalformedEventBytes)?;
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index)
            || event.pallet_name() != "Subaccount"
            || event.variant_name() != "PerpOrderModified"
        {
            continue;
        }
        if evidence.is_some() {
            return Err(Error::ConflictingEvidence);
        }
        let fields = event
            .field_values()
            .map_err(|_| Error::MalformedEventBytes)?;
        let Composite::Named(fields) = fields else {
            return Err(Error::MalformedEventBytes);
        };
        let field = |name| {
            fields
                .iter()
                .find_map(|(candidate, value)| (candidate == name).then_some(value))
        };
        let Some(filled_size) = field("filled_size").and_then(ScaleValue::as_u128) else {
            return Err(Error::MalformedEventBytes);
        };
        let Some(event_target_size) = field("target_size").and_then(ScaleValue::as_u128) else {
            return Err(Error::MalformedEventBytes);
        };
        let Some(replacement_size) = field("replacement_size").and_then(ScaleValue::as_u128) else {
            return Err(Error::MalformedEventBytes);
        };
        let Some(old_order_id) = field("old_order_id").and_then(ScaleValue::as_u128) else {
            return Err(Error::MalformedEventBytes);
        };
        let Some(new_order_id) = field("new_order_id").and_then(ScaleValue::as_u128) else {
            return Err(Error::MalformedEventBytes);
        };
        if !field("subaccount").is_some_and(|value| value_matches_bytes(value, subaccount))
            || old_order_id != u128::from(*cancel_order_id)
            || event_target_size != *target_size
            || new_order_id != u128::from(expected_new_order_id)
            || target_size.checked_sub(filled_size) != Some(replacement_size)
            || replacement_size == 0
        {
            return Err(Error::ConflictingEvidence);
        }
        evidence = Some(replacement_size);
    }
    evidence.ok_or(Error::ConflictingEvidence)
}

fn perp_modify_events_are_ordered(
    snapshot: &impl RuntimeMetadataView,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<bool, DeepXPerpModifyEventVerificationError> {
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    let mut cancel_position = None;
    let mut place_position = None;
    let mut modify_position = None;
    for (position, event) in events.iter().enumerate() {
        let event =
            event.map_err(|_| DeepXPerpModifyEventVerificationError::MalformedEventBytes)?;
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index) {
            continue;
        }
        if event.pallet_name() == "Subaccount" && event.variant_name() == "PerpOrderModified" {
            modify_position = Some(position);
            continue;
        }
        if event.pallet_name() != "PerpMarket" {
            continue;
        }
        match event.variant_name() {
            "OrderCancelled" => cancel_position = Some(position),
            "OrderPlaced" => place_position = Some(position),
            _ => {}
        }
    }
    Ok(cancel_position
        .zip(place_position)
        .is_some_and(|(cancel, place)| cancel < place)
        && place_position
            .zip(modify_position)
            .is_some_and(|(place, modify)| place < modify))
}

/// Verifies the expected business event for one durable perpetual cancel operation.
///
/// The event bytes must be the complete SCALE value returned from `System.Events`, including its
/// compact event-count prefix. Only `ApplyExtrinsic` events at `extrinsic_index` are considered.
/// Fast cancel remains unsupported because the runtime deliberately suppresses this pallet event.
///
/// # Errors
///
/// Returns an error when the runtime interface is unavailable, the operation cannot emit
/// authoritative evidence, event decoding fails, or cancellation evidence conflicts or repeats.
pub fn verify_perp_cancel_business_event(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXIndexedOutcome<DeepXBusinessEventOutcome>, DeepXPerpCancelEventVerificationError> {
    verify_perp_cancel_business_event_with_runtime(snapshot, identity, extrinsic_index, event_bytes)
}

fn verify_perp_cancel_business_event_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    identity: &DeepXTransactionIdentity,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXIndexedOutcome<DeepXBusinessEventOutcome>, DeepXPerpCancelEventVerificationError> {
    snapshot
        .interfaces()
        .event("PerpMarket", "OrderCancelled")?;
    let Some(DeepXTransactionOperation::PerpCancel {
        subaccount,
        order_id,
        fast_cancel,
        ..
    }) = identity.operation()
    else {
        return Err(DeepXPerpCancelEventVerificationError::UnsupportedOperation);
    };
    if *fast_cancel {
        return Err(DeepXPerpCancelEventVerificationError::FastCancelUnsupported);
    }

    let mut remaining = event_bytes;
    let declared_count = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXPerpCancelEventVerificationError::MalformedEventBytes)?
        .0;
    let prefix_len = event_bytes.len() - remaining.len();
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    let mut matched = false;
    let mut decoded_count = 0_u32;
    let mut consumed_len = prefix_len;
    for event in events.iter() {
        let event =
            event.map_err(|e| DeepXPerpCancelEventVerificationError::Decode(Box::new(e)))?;
        decoded_count += 1;
        consumed_len += event.bytes().len();
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index)
            || event.pallet_name() != "PerpMarket"
            || event.variant_name() != "OrderCancelled"
        {
            continue;
        }
        let fields = event
            .field_values()
            .map_err(|e| DeepXPerpCancelEventVerificationError::Decode(Box::new(e)))?;
        if !perp_cancel_fields_match(&fields, *subaccount, *order_id) {
            return Err(DeepXPerpCancelEventVerificationError::ConflictingEvent);
        }
        if matched {
            return Err(DeepXPerpCancelEventVerificationError::DuplicateEvent);
        }
        matched = true;
    }
    if decoded_count != declared_count || consumed_len != event_bytes.len() {
        return Err(DeepXPerpCancelEventVerificationError::MalformedEventBytes);
    }

    Ok(DeepXIndexedOutcome {
        extrinsic_index,
        outcome: if matched {
            DeepXBusinessEventOutcome::Success
        } else {
            DeepXBusinessEventOutcome::NotObserved
        },
    })
}

fn verify_perp_cancel_inclusion_events(
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpCancelEventVerificationError> {
    verify_perp_cancel_inclusion_events_with_runtime(
        snapshot,
        identity,
        block_hash,
        block_number,
        extrinsic_index,
        event_bytes,
    )
}

fn verify_perp_cancel_inclusion_events_with_runtime(
    snapshot: &impl RuntimeMetadataView,
    identity: &DeepXTransactionIdentity,
    block_hash: [u8; 32],
    block_number: u64,
    extrinsic_index: u32,
    event_bytes: &[u8],
) -> Result<DeepXInclusionEvidence, DeepXPerpCancelEventVerificationError> {
    let Some(DeepXTransactionOperation::PerpCancel { fast_cancel, .. }) = identity.operation()
    else {
        return Err(DeepXPerpCancelEventVerificationError::UnsupportedOperation);
    };
    snapshot.interfaces().event("System", "ExtrinsicSuccess")?;
    snapshot.interfaces().event("System", "ExtrinsicFailed")?;

    let mut remaining = event_bytes;
    let declared_count = Compact::<u32>::decode(&mut remaining)
        .map_err(|_| DeepXPerpCancelEventVerificationError::MalformedEventBytes)?
        .0;
    let prefix_len = event_bytes.len() - remaining.len();
    let events = Events::<DeepXRuntimeConfig>::decode_from(
        event_bytes.to_vec(),
        snapshot.metadata().clone(),
    );
    let mut dispatch = None;
    let mut decoded_count = 0_u32;
    let mut consumed_len = prefix_len;
    for event in events.iter() {
        let event =
            event.map_err(|e| DeepXPerpCancelEventVerificationError::Decode(Box::new(e)))?;
        decoded_count += 1;
        consumed_len += event.bytes().len();
        if event.phase() != Phase::ApplyExtrinsic(extrinsic_index)
            || event.pallet_name() != "System"
        {
            continue;
        }
        let outcome = match event.variant_name() {
            "ExtrinsicSuccess" => DeepXDispatchOutcome::Success,
            "ExtrinsicFailed" => DeepXDispatchOutcome::Failed,
            _ => continue,
        };
        if dispatch.replace(outcome).is_some() {
            return Err(DeepXPerpCancelEventVerificationError::DuplicateEvent);
        }
    }
    if decoded_count != declared_count || consumed_len != event_bytes.len() {
        return Err(DeepXPerpCancelEventVerificationError::MalformedEventBytes);
    }
    let dispatch = DeepXIndexedOutcome {
        extrinsic_index,
        outcome: dispatch.ok_or(DeepXPerpCancelEventVerificationError::MalformedEventBytes)?,
    };
    if *fast_cancel {
        return Ok(DeepXInclusionEvidence {
            block_hash,
            block_number,
            extrinsic_index,
            outcome: match dispatch.outcome {
                DeepXDispatchOutcome::Success => super::DeepXInclusionOutcome::Success,
                DeepXDispatchOutcome::Failed => super::DeepXInclusionOutcome::Failed,
            },
        });
    }
    let business_event = verify_perp_cancel_business_event_with_runtime(
        snapshot,
        identity,
        extrinsic_index,
        event_bytes,
    )?;
    Ok(DeepXInclusionEvidence::from_indexed_observations(
        block_hash,
        block_number,
        dispatch,
        business_event,
    )?)
}

fn perp_cancel_fields_match(
    fields: &Composite<u32>,
    expected_subaccount: [u8; 20],
    expected_order_id: u64,
) -> bool {
    let Composite::Named(fields) = fields else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    field("user").is_some_and(|value| value_matches_bytes(value, &expected_subaccount))
        && field("order_id").and_then(ScaleValue::as_u128) == Some(u128::from(expected_order_id))
        && field("reason").is_some_and(|value| {
            matches!(&value.value, ValueDef::Variant(reason) if reason.name == "UserCanceled")
        })
}

fn value_matches_bytes(value: &ScaleValue<u32>, expected: &[u8]) -> bool {
    let ValueDef::Composite(bytes) = &value.value else {
        return false;
    };
    let values: Vec<_> = bytes.values().collect();
    if values.len() == 1 {
        return value_matches_bytes(values[0], expected);
    }
    values.into_iter().map(ScaleValue::as_u128).eq(expected
        .iter()
        .copied()
        .map(u128::from)
        .map(Some))
}

#[allow(clippy::too_many_arguments)]
fn spot_place_order_matches(
    value: &ScaleValue<u32>,
    subaccount: [u8; 20],
    pair: [u8; 32],
    order_id: u64,
    quote_amount: [u8; 32],
    base_amount: [u8; 32],
    order_type: crate::signing::DeepXSpotOrderType,
    post_only: crate::signing::DeepXPostOnlyParam,
    reduce_only: bool,
    is_buy: bool,
) -> bool {
    let ValueDef::Composite(Composite::Named(fields)) = &value.value else {
        return false;
    };
    let field = |name| {
        fields
            .iter()
            .find_map(|(candidate, value)| (candidate == name).then_some(value))
    };
    fields.len() == 12
        && field("id").and_then(ScaleValue::as_u128) == Some(u128::from(order_id))
        && field("maker").is_some_and(|value| value_matches_bytes(value, &subaccount))
        && field("pair").is_some_and(|value| value_matches_bytes(value, &pair))
        && field("price").is_some()
        && field("quote_amount").is_some_and(|value| value_matches_u256_le(value, &quote_amount))
        && field("base_amount").is_some_and(|value| value_matches_u256_le(value, &base_amount))
        && field("create_time").is_some()
        && field("status").is_some()
        && field("order_type").is_some_and(|value| value_matches_order_type(value, order_type))
        && field("post_only").is_some_and(|value| value_matches_post_only(value, post_only))
        && field("reduce_only").and_then(ScaleValue::as_bool) == Some(reduce_only)
        && field("is_buy").and_then(ScaleValue::as_bool) == Some(is_buy)
}

fn value_matches_u256_le(value: &ScaleValue<u32>, expected: &[u8; 32]) -> bool {
    let ValueDef::Composite(composite) = &value.value else {
        return false;
    };
    let values: Vec<_> = composite.values().collect();
    if values.len() == 1 {
        return value_matches_u256_le(values[0], expected);
    }
    values.len() == 4
        && values.iter().enumerate().all(|(index, value)| {
            let offset = index * 8;
            let expected_limb = u64::from_le_bytes(
                expected[offset..offset + 8]
                    .try_into()
                    .expect("fixed U256 limb"),
            );
            value.as_u128() == Some(u128::from(expected_limb))
        })
}

fn value_matches_order_type(
    value: &ScaleValue<u32>,
    expected: crate::signing::DeepXSpotOrderType,
) -> bool {
    let ValueDef::Variant(variant) = &value.value else {
        return false;
    };
    match expected {
        crate::signing::DeepXSpotOrderType::Limit(time_in_force) => {
            let values: Vec<_> = variant.values.values().collect();
            variant.name == "Limit"
                && values.len() == 1
                && matches!(&values[0].value, ValueDef::Variant(value)
                    if value.name == match time_in_force {
                        crate::signing::DeepXTimeInForce::Gtc => "GTC",
                        crate::signing::DeepXTimeInForce::Ioc => "IOC",
                        crate::signing::DeepXTimeInForce::Fok => "FOK",
                    } && value.values.values().next().is_none())
        }
        crate::signing::DeepXSpotOrderType::Market(slippage) => {
            let values: Vec<_> = variant.values.values().collect();
            if variant.name != "Market" || values.len() != 1 {
                return false;
            }
            let ValueDef::Variant(option) = &values[0].value else {
                return false;
            };
            let option_values: Vec<_> = option.values.values().collect();
            match slippage {
                Some(expected) => {
                    option.name == "Some"
                        && option_values.len() == 1
                        && option_values[0].as_u128() == Some(u128::from(expected))
                }
                None => option.name == "None" && option_values.is_empty(),
            }
        }
        crate::signing::DeepXSpotOrderType::Stop => {
            variant.name == "Stop" && variant.values.values().next().is_none()
        }
    }
}

fn value_matches_post_only(
    value: &ScaleValue<u32>,
    expected: crate::signing::DeepXPostOnlyParam,
) -> bool {
    let ValueDef::Variant(variant) = &value.value else {
        return false;
    };
    let expected = match expected {
        crate::signing::DeepXPostOnlyParam::None => "None",
        crate::signing::DeepXPostOnlyParam::MustPostOnly => "MustPostOnly",
        crate::signing::DeepXPostOnlyParam::Adaptive => "Adaptive",
    };
    variant.name == expected && variant.values.values().next().is_none()
}

/// Errors raised while observing transaction presence through DeepX RPC endpoints.
#[derive(Debug, Error)]
pub enum DeepXTransactionWatchError {
    /// A request or JSON-RPC response failed.
    #[error("failed to observe DeepX transaction using `{method}`: {source}")]
    Rpc {
        /// Method which failed.
        method: &'static str,
        /// Underlying request or response error.
        #[source]
        source: anyhow::Error,
    },
    /// The node returned a malformed block hash.
    #[error("DeepX canonical block returned an invalid block hash: {0}")]
    InvalidBlockHash(String),
    /// The requested canonical block was unavailable.
    #[error("DeepX canonical block {0} was unavailable")]
    BlockUnavailable(u64),
    /// The finalized head did not have an available header.
    #[error("DeepX finalized head header was unavailable")]
    FinalizedHeaderUnavailable,
    /// Finality reached the recorded height without preserving the exact recorded inclusion.
    #[error(
        "DeepX finalized chain does not preserve the recorded transaction inclusion at block {0}"
    )]
    FinalityEvidenceConflict(u64),
    /// The durable recovery checkpoint is no longer canonical at its recorded height.
    #[error(
        "DeepX recovery checkpoint at block {block_number} no longer matches the canonical hash"
    )]
    RecoveryCheckpointMismatch {
        /// Durable checkpoint block number.
        block_number: u64,
        /// Hash retained with the durable checkpoint.
        expected: [u8; 32],
        /// Current canonical hash at the checkpoint height.
        received: [u8; 32],
    },
    /// The returned block header did not identify the requested height.
    #[error("DeepX canonical block number mismatch: requested {requested}, received {received}")]
    BlockNumberMismatch {
        /// Requested canonical block number.
        requested: u64,
        /// Block number decoded from the returned header.
        received: u64,
    },
    /// A block or pool entry was not a prefixed SCALE extrinsic.
    #[error("DeepX {evidence_source} extrinsic at index {index} was invalid")]
    InvalidExtrinsic {
        /// Evidence source containing the malformed entry.
        evidence_source: &'static str,
        /// Zero-based entry index.
        index: usize,
    },
    /// A node returned the target extrinsic more than once.
    #[error("DeepX {evidence_source} contained duplicate target extrinsics")]
    DuplicateExtrinsic {
        /// Evidence source containing duplicate entries.
        evidence_source: &'static str,
    },
    /// The block header number was malformed or outside `u64`.
    #[error("DeepX canonical block returned an invalid block number: {0}")]
    InvalidBlockNumber(String),
    /// RPC capability evidence does not belong to the selected endpoint or is incomplete.
    #[error(
        "DeepX RPC capability evidence for role {0:?} does not match the validated endpoint set"
    )]
    CapabilitiesMismatch(DeepXRpcRole),
    /// Exact inclusion cannot be classified without authoritative event evidence.
    #[error(
        "DeepX transaction was found in canonical block {block_number} at extrinsic index {extrinsic_index}, but authoritative event evidence is unavailable"
    )]
    EventEvidenceUnavailable {
        /// Canonical block containing the exact target extrinsic.
        block_number: u64,
        /// Index of the exact target extrinsic in the block.
        extrinsic_index: u32,
    },
    /// A finalized canonical block did not contain the exact target extrinsic.
    #[error("DeepX finalized block {0} does not contain the target extrinsic")]
    FinalizedExtrinsicUnavailable(u64),
    /// The exact finalized block did not expose complete system event storage.
    #[error("DeepX system event storage was unavailable at finalized block {0}")]
    EventStorageUnavailable(u64),
    /// The exact finalized block returned malformed system event storage.
    #[error("DeepX system event storage at finalized block {0} was malformed")]
    InvalidEventStorage(u64),
    /// Runtime event evidence did not prove the durable perpetual cancellation.
    #[error(transparent)]
    EventVerification(#[from] DeepXPerpCancelEventVerificationError),
    /// Runtime event evidence did not prove the explicitly selected Spot cancellation.
    #[error(transparent)]
    SpotEventVerification(#[from] DeepXSpotCancelEventVerificationError),
    /// Runtime event evidence did not prove the explicitly selected Spot placement.
    #[error(transparent)]
    SpotPlaceEventVerification(#[from] DeepXSpotPlaceEventVerificationError),
    /// Runtime events could not establish the durable perpetual placement.
    #[error(transparent)]
    PerpPlaceEventVerification(#[from] DeepXPerpPlaceEventVerificationError),
    /// Close-generated order evidence conflicts with durable call terms.
    #[error(transparent)]
    PerpCloseEventVerification(#[from] DeepXPerpCloseEventVerificationError),
    /// Perpetual TP/SL update evidence did not match the durable operation.
    #[error(transparent)]
    PerpProfitAndLossPointEventVerification(
        #[from] DeepXPerpProfitAndLossPointEventVerificationError,
    ),
    /// Both perpetual modify legs could not be verified at the exact index.
    #[error(transparent)]
    PerpModifyEventVerification(#[from] DeepXPerpModifyEventVerificationError),
    /// A bounded recovery scan could not be planned safely.
    #[error(transparent)]
    ScanPlan(#[from] DeepXRecoveryScanPlanError),
    /// Collected canonical evidence did not match the bounded scan plan.
    #[error(transparent)]
    ScanCollection(#[from] DeepXRecoveryScanCollectionError),
}

#[derive(Debug, Deserialize)]
struct RpcBlockResponse {
    block: RpcBlock,
}

#[derive(Debug, Deserialize)]
struct RpcBlock {
    header: RpcBlockHeader,
    extrinsics: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RpcBlockHeader {
    number: String,
}

/// Exact transaction-location observation from one canonical block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXCanonicalBlockObservation {
    block_number: u64,
    block_hash: [u8; 32],
    extrinsic_index: Option<u32>,
}

/// Finality observation for one exact recorded inclusion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXFinalityObservation {
    /// The finalized head has not reached the recorded inclusion height.
    Pending(DeepXFinalizedRecoveryCheckpoint),
    /// The exact recorded inclusion remains canonical at a finalized height.
    Finalized(DeepXInclusionEvidence),
}

/// Exact location of one extrinsic in a finalized canonical block.
///
/// This proves transaction membership and finality, but not dispatch or business outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedTransactionLocation {
    extrinsic_hash: [u8; 32],
    block_number: u64,
    block_hash: [u8; 32],
    extrinsic_index: u32,
}

impl DeepXFinalizedTransactionLocation {
    /// Returns the exact extrinsic hash located in the finalized block.
    #[must_use]
    pub const fn extrinsic_hash(&self) -> [u8; 32] {
        self.extrinsic_hash
    }

    /// Returns the finalized block number containing the extrinsic.
    #[must_use]
    pub const fn block_number(&self) -> u64 {
        self.block_number
    }

    /// Returns the finalized canonical block hash.
    #[must_use]
    pub const fn block_hash(&self) -> [u8; 32] {
        self.block_hash
    }

    /// Returns the unique extrinsic index in the finalized block.
    #[must_use]
    pub const fn extrinsic_index(&self) -> u32 {
        self.extrinsic_index
    }
}

/// Finalized-location observation for an exact transaction and reported block height.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXFinalizedTransactionLocationObservation {
    /// The finalized head has not reached the requested block height.
    Pending(DeepXFinalizedRecoveryCheckpoint),
    /// The exact target extrinsic was found uniquely in the finalized canonical block.
    Finalized(DeepXFinalizedTransactionLocation),
}

impl DeepXCanonicalBlockObservation {
    /// Returns the canonical block number inspected by the watch endpoint.
    #[must_use]
    pub const fn block_number(&self) -> u64 {
        self.block_number
    }

    /// Returns the canonical block hash supplied by the watch endpoint.
    #[must_use]
    pub const fn block_hash(&self) -> [u8; 32] {
        self.block_hash
    }

    /// Returns the unique extrinsic index whose recomputed hash matched the target.
    #[must_use]
    pub const fn extrinsic_index(&self) -> Option<u32> {
        self.extrinsic_index
    }
}

/// Classifies a recorded non-finalized inclusion against an exact canonical block observation.
///
/// A different block hash at the recorded height proves a reorganization. An unchanged block is
/// canonical only when the target extrinsic remains at the recorded index. Missing or displaced
/// transaction evidence requires operator action rather than an inferred absence.
#[must_use]
fn classify_canonical_block_observation(
    recorded_inclusion: DeepXInclusionEvidence,
    observation: DeepXCanonicalBlockObservation,
) -> DeepXReorganizationDecision {
    let inclusion = (observation.block_hash() == recorded_inclusion.block_hash()
        && observation.extrinsic_index() == Some(recorded_inclusion.extrinsic_index()))
    .then_some(recorded_inclusion);
    classify_reorganization(
        recorded_inclusion,
        Some(DeepXCanonicalBlockEvidence::new(
            observation.block_number(),
            observation.block_hash(),
            inclusion,
        )),
    )
}

/// Observes whether a recorded non-finalized inclusion remains canonical at its exact height.
///
/// The durable extrinsic hash binds the canonical block lookup to the recorded transaction. This
/// boundary performs no lifecycle mutation and grants no replay or replacement authority.
///
/// # Errors
///
/// Returns an error for mismatched Watch capabilities, RPC failure, malformed or inconsistent
/// block identity, malformed extrinsics, or duplicate target entries.
pub async fn observe_reorganization(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    target_extrinsic_hash: [u8; 32],
    recorded_inclusion: DeepXInclusionEvidence,
) -> Result<DeepXReorganizationDecision, DeepXTransactionWatchError> {
    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != watch_url
        || DEEPX_WATCH_RPC_METHODS
            .iter()
            .any(|method| !watch_capabilities.methods().contains(*method))
    {
        return Err(DeepXTransactionWatchError::CapabilitiesMismatch(
            DeepXRpcRole::Watch,
        ));
    }

    let observation = observe_canonical_block_at(
        watch_url,
        recorded_inclusion.block_number(),
        target_extrinsic_hash,
    )
    .await?;
    Ok(classify_canonical_block_observation(
        recorded_inclusion,
        observation,
    ))
}

/// Observes whether an exact recorded inclusion has become final on the Watch endpoint.
///
/// Finality is released only when the finalized head reaches the recorded height and the same
/// block hash still contains the target extrinsic at its recorded index. Conflicting canonical
/// evidence is rejected for separate reorganization handling.
///
/// # Errors
///
/// Returns an error for mismatched Watch capabilities, RPC failure, malformed chain data, or
/// canonical evidence which no longer preserves the exact recorded inclusion.
pub async fn observe_finality(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    target_extrinsic_hash: [u8; 32],
    recorded_inclusion: DeepXInclusionEvidence,
) -> Result<DeepXFinalityObservation, DeepXTransactionWatchError> {
    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != watch_url
        || DEEPX_WATCH_RPC_METHODS
            .iter()
            .any(|method| !watch_capabilities.methods().contains(*method))
    {
        return Err(DeepXTransactionWatchError::CapabilitiesMismatch(
            DeepXRpcRole::Watch,
        ));
    }

    let checkpoint = observe_finalized_checkpoint_at(watch_url).await?;
    if checkpoint.block_number() < recorded_inclusion.block_number() {
        return Ok(DeepXFinalityObservation::Pending(checkpoint));
    }

    let observation = observe_canonical_block_at(
        watch_url,
        recorded_inclusion.block_number(),
        target_extrinsic_hash,
    )
    .await?;
    match classify_canonical_block_observation(recorded_inclusion, observation) {
        DeepXReorganizationDecision::Canonical => {
            Ok(DeepXFinalityObservation::Finalized(recorded_inclusion))
        }
        DeepXReorganizationDecision::Reorganized(_)
        | DeepXReorganizationDecision::ActionRequired => Err(
            DeepXTransactionWatchError::FinalityEvidenceConflict(recorded_inclusion.block_number()),
        ),
    }
}

/// Locates an exact extrinsic at a requested height after that height becomes finalized.
///
/// This read-only boundary verifies the Watch endpoint capabilities, observes its finalized head,
/// and recomputes every extrinsic hash in the canonical block at the requested height. It does not
/// inspect events or infer dispatch or business success.
///
/// # Errors
///
/// Returns an error for mismatched capabilities, malformed chain data, duplicate target
/// extrinsics, or a finalized canonical block which does not contain the exact target.
pub async fn observe_finalized_transaction_location(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    target_extrinsic_hash: [u8; 32],
    block_number: u64,
) -> Result<DeepXFinalizedTransactionLocationObservation, DeepXTransactionWatchError> {
    let watch_url = endpoints.url_for(DeepXRpcRole::Watch);
    let watch_capabilities = capabilities.for_role(DeepXRpcRole::Watch);
    if watch_capabilities.role() != DeepXRpcRole::Watch
        || watch_capabilities.endpoint_url() != watch_url
        || DEEPX_WATCH_RPC_METHODS
            .iter()
            .any(|method| !watch_capabilities.methods().contains(*method))
    {
        return Err(DeepXTransactionWatchError::CapabilitiesMismatch(
            DeepXRpcRole::Watch,
        ));
    }

    let checkpoint = observe_finalized_checkpoint_at(watch_url).await?;
    if checkpoint.block_number() < block_number {
        return Ok(DeepXFinalizedTransactionLocationObservation::Pending(
            checkpoint,
        ));
    }

    let observation =
        observe_canonical_block_at(watch_url, block_number, target_extrinsic_hash).await?;
    let extrinsic_index = observation.extrinsic_index().ok_or(
        DeepXTransactionWatchError::FinalizedExtrinsicUnavailable(block_number),
    )?;
    Ok(DeepXFinalizedTransactionLocationObservation::Finalized(
        DeepXFinalizedTransactionLocation {
            extrinsic_hash: target_extrinsic_hash,
            block_number,
            block_hash: observation.block_hash(),
            extrinsic_index,
        },
    ))
}

/// Exact transaction-membership observation from the submission node pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXPoolObservation {
    /// The exact target extrinsic is present in the pending pool.
    Present,
    /// Every returned pending extrinsic was valid and none matched the target.
    Absent,
}

/// Finalized chain checkpoint observed from the recovery endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXFinalizedRecoveryCheckpoint {
    block_number: u64,
    block_hash: [u8; 32],
}

impl DeepXFinalizedRecoveryCheckpoint {
    /// Creates a finalized checkpoint to verify against canonical chain evidence.
    #[must_use]
    pub const fn new(block_number: u64, block_hash: [u8; 32]) -> Self {
        Self {
            block_number,
            block_hash,
        }
    }

    /// Returns the finalized block number.
    #[must_use]
    pub const fn block_number(&self) -> u64 {
        self.block_number
    }

    /// Returns the finalized block hash.
    #[must_use]
    pub const fn block_hash(&self) -> [u8; 32] {
        self.block_hash
    }
}

async fn observe_finalized_checkpoint_at(
    endpoint_url: &str,
) -> Result<DeepXFinalizedRecoveryCheckpoint, DeepXTransactionWatchError> {
    let client = BlockchainHttpRpcClient::new(endpoint_url.to_string(), None, None);
    let encoded_hash: String = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getFinalizedHead",
            "params": [],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getFinalizedHead",
            source,
        })?;
    let block_hash = decode_hash(&encoded_hash)?;
    let header: Option<RpcBlockHeader> = client
        .execute_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getHeader",
            "params": [encoded_hash],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "chain_getHeader",
            source,
        })?;
    let header = header.ok_or(DeepXTransactionWatchError::FinalizedHeaderUnavailable)?;
    Ok(DeepXFinalizedRecoveryCheckpoint {
        block_number: decode_block_number(&header.number)?,
        block_hash,
    })
}

/// Complete result of observing a bounded finalized recovery interval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeepXFinalizedRecoveryCollection {
    /// The durable scan checkpoint already equals the current finalized head.
    UpToDate(DeepXFinalizedRecoveryCheckpoint),
    /// Every planned canonical block and the pending pool were inspected successfully.
    Scan(DeepXRecoveryScan),
}

/// Observes one canonical block and locates a transaction by its recomputed extrinsic hash.
///
/// This read-only boundary proves only exact transaction presence at an extrinsic index. It does
/// not decode dispatch or business events and therefore cannot produce inclusion success, failure,
/// or finality evidence.
///
/// # Errors
///
/// Returns an error for RPC failure, malformed or inconsistent block identity, malformed
/// extrinsics, duplicate target entries, or an index outside `u32`.
pub async fn observe_canonical_block(
    endpoints: &DeepXValidatedRpcEndpoints,
    block_number: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXCanonicalBlockObservation, DeepXTransactionWatchError> {
    observe_canonical_block_at(
        endpoints.url_for(DeepXRpcRole::Watch),
        block_number,
        target_extrinsic_hash,
    )
    .await
}

async fn observe_canonical_block_at(
    endpoint_url: &str,
    block_number: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXCanonicalBlockObservation, DeepXTransactionWatchError> {
    let client = BlockchainHttpRpcClient::new(endpoint_url.to_string(), None, None);
    let encoded_hash: Option<String> = execute_optional_rpc_read_with_retry(
        &client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [block_number],
        }),
    )
    .await
    .map_err(|source| DeepXTransactionWatchError::Rpc {
        method: "chain_getBlockHash",
        source,
    })?;
    let encoded_hash =
        encoded_hash.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?;
    let block_hash = decode_hash(&encoded_hash)?;
    let response: Option<RpcBlockResponse> = execute_optional_rpc_read_with_retry(
        &client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlock",
            "params": [encoded_hash],
        }),
    )
    .await
    .map_err(|source| DeepXTransactionWatchError::Rpc {
        method: "chain_getBlock",
        source,
    })?;
    let response = response.ok_or(DeepXTransactionWatchError::BlockUnavailable(block_number))?;
    let observed_number = decode_block_number(&response.block.header.number)?;
    if observed_number != block_number {
        return Err(DeepXTransactionWatchError::BlockNumberMismatch {
            requested: block_number,
            received: observed_number,
        });
    }
    let extrinsic_index = find_extrinsic(
        &response.block.extrinsics,
        target_extrinsic_hash,
        "canonical block",
    )?;

    Ok(DeepXCanonicalBlockObservation {
        block_number,
        block_hash,
        extrinsic_index,
    })
}

/// Collects a complete bounded canonical scan through the recovery endpoint's finalized head.
///
/// The supplied capability evidence must belong to the validated Recovery endpoint and include
/// every method required by the recovery flow. Exact transaction absence can then be classified by
/// [`DeepXRecoveryScan::classify`]. Finding the target extrinsic stops collection because block
/// location without authoritative dispatch and business-event evidence cannot construct a trusted
/// inclusion outcome.
///
/// # Errors
///
/// Returns an error for mismatched capabilities, RPC or decoding failure, an invalid scan plan,
/// inconsistent range collection, or target inclusion without authoritative event evidence.
pub async fn collect_finalized_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, DeepXTransactionWatchError> {
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        None,
    )
    .await
}

/// Collects canonical finalized evidence for an explicitly selected ordinary Spot cancel.
///
/// This read-only boundary uses the approved snapshot and durable operation to verify dispatch
/// and business events at the exact canonical block and extrinsic index. It does not commit,
/// submit, replay, or enable execution recovery. Non-atomic pool absence remains unknown.
///
/// # Errors
///
/// Returns an error for a foreign runtime, unsupported or fast operation, invalid canonical
/// checkpoint, RPC failure, or incomplete or conflicting inclusion event evidence.
pub async fn collect_finalized_spot_cancel_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned: DeepXFinalizedRecoveryCheckpoint,
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, DeepXTransactionWatchError> {
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.operation(),
            Some(DeepXTransactionOperation::SpotCancel { .. })
        )
    {
        return Err(DeepXSpotCancelEventVerificationError::UnsupportedOperation.into());
    }
    if matches!(
        identity.operation(),
        Some(DeepXTransactionOperation::SpotCancel {
            fast_cancel: true,
            ..
        })
    ) {
        return Err(DeepXSpotCancelEventVerificationError::FastCancelUnsupported.into());
    }
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned.block_number(),
        last_scanned.block_hash(),
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::Spot(snapshot, identity)),
    )
    .await
}

/// Collects canonical finalized evidence for an explicitly selected Spot placement.
///
/// This read-only boundary verifies dispatch and the exact side-specific order state event. It
/// does not commit, submit, replay, or enable Spot execution.
///
/// # Errors
///
/// Returns an error for a foreign runtime, unsupported operation, invalid canonical checkpoint,
/// RPC failure, or incomplete or conflicting inclusion event evidence.
pub async fn collect_finalized_spot_place_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned: DeepXFinalizedRecoveryCheckpoint,
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, DeepXTransactionWatchError> {
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.operation(),
            Some(DeepXTransactionOperation::SpotPlace { .. })
        )
    {
        return Err(DeepXSpotPlaceEventVerificationError::UnsupportedOperation.into());
    }
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned.block_number(),
        last_scanned.block_hash(),
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::SpotPlace(snapshot, identity)),
    )
    .await
}

enum CancelEventEvidence<'a> {
    Perp(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
    Spot(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
    SpotPlace(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
    PerpPlace(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
    PerpClose(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
    PerpProfitAndLossPoint(&'a RuntimeSnapshot, &'a DeepXTransactionIdentity),
    PerpModify(
        &'a RuntimeSnapshot,
        &'a DeepXTransactionIdentity,
        DeepXPerpModifyParams,
    ),
}

/// Collects bounded canonical finalized evidence for a perpetual modify operation.
#[expect(
    clippy::too_many_arguments,
    reason = "matches bounded recovery scan inputs"
)]
pub async fn collect_finalized_perp_modify_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, Box<DeepXTransactionWatchError>> {
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.operation(),
            Some(DeepXTransactionOperation::PerpModify { .. })
        )
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(Box::new(
            DeepXPerpModifyEventVerificationError::UnsupportedOperation.into(),
        ));
    }
    let Some(DeepXTransactionOperation::PerpModify {
        subaccount,
        place_subaccount,
        cancel_order_id,
        market_id,
        place_market_id,
        fast_cancel,
        is_long,
        size,
        price,
        order_type,
        take_profit,
        stop_loss,
        reduce_only,
        post_only,
    }) = identity.operation()
    else {
        return Err(Box::new(
            DeepXPerpModifyEventVerificationError::UnsupportedOperation.into(),
        ));
    };
    let params = DeepXPerpModifyParams {
        cancel: crate::signing::DeepXPerpCancelParams {
            subaccount: *subaccount,
            order_id: *cancel_order_id,
            market_id: *market_id,
            fast_cancel: *fast_cancel,
        },
        place: crate::signing::DeepXPerpPlaceParams {
            subaccount: *place_subaccount,
            market_id: *place_market_id,
            is_long: *is_long,
            size: *size,
            price: *price,
            order_type: *order_type,
            take_profit: *take_profit,
            stop_loss: *stop_loss,
            reduce_only: *reduce_only,
            post_only: *post_only,
        },
    };
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::PerpModify(snapshot, identity, params)),
    )
    .await
    .map_err(Box::new)
}

/// Collects bounded canonical finalized evidence for a close-generated perpetual order.
///
/// # Errors
///
/// Returns an error for unsupported scope, RPC failure or conflicting order evidence.
#[expect(
    clippy::too_many_arguments,
    reason = "matches bounded recovery scan inputs"
)]
pub async fn collect_finalized_perp_close_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, Box<DeepXTransactionWatchError>> {
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.operation(),
            Some(DeepXTransactionOperation::PerpClose { .. })
        )
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(Box::new(
            DeepXPerpCloseEventVerificationError::UnsupportedOperation.into(),
        ));
    }
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::PerpClose(snapshot, identity)),
    )
    .await
    .map_err(Box::new)
}

/// Collects bounded finalized recovery evidence for an explicit TP/SL update.
///
/// # Errors
///
/// Returns an error for unsupported scope, RPC failure or conflicting indexed events.
#[expect(
    clippy::too_many_arguments,
    reason = "matches bounded recovery scan inputs"
)]
pub async fn collect_finalized_perp_profit_and_loss_point_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, Box<DeepXTransactionWatchError>> {
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.operation(),
            Some(DeepXTransactionOperation::PerpProfitAndLossPoint { .. })
        )
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(Box::new(
            DeepXPerpProfitAndLossPointEventVerificationError::UnsupportedOperation.into(),
        ));
    }
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::PerpProfitAndLossPoint(
            snapshot, identity,
        )),
    )
    .await
    .map_err(Box::new)
}

/// Collects bounded finalized recovery evidence for an explicit perpetual placement.
///
/// # Errors
///
/// Returns an error for unsupported operation/runtime scope, RPC failure, or conflicting events.
#[expect(
    clippy::too_many_arguments,
    reason = "matches existing bounded recovery scan inputs"
)]
pub async fn collect_finalized_perp_place_recovery_scan(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, Box<DeepXTransactionWatchError>> {
    if identity.runtime() != &super::DeepXDirectRuntimeIdentity::from(snapshot.identity())
        || !matches!(
            identity.operation(),
            Some(DeepXTransactionOperation::PerpPlace { .. })
        )
        || !matches!(
            identity.nonce(),
            super::DeepXNonceReservation::TimestampOrderId { .. }
        )
    {
        return Err(Box::new(
            DeepXPerpPlaceEventVerificationError::UnsupportedOperation.into(),
        ));
    }
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned_block,
        last_scanned_block_hash,
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::PerpPlace(snapshot, identity)),
    )
    .await
    .map_err(Box::new)
}

pub(crate) async fn collect_finalized_recovery_scan_with_event_evidence(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    snapshot: &RuntimeSnapshot,
    identity: &DeepXTransactionIdentity,
    last_scanned: DeepXFinalizedRecoveryCheckpoint,
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXFinalizedRecoveryCollection, DeepXTransactionWatchError> {
    collect_finalized_recovery_scan_inner(
        endpoints,
        capabilities,
        last_scanned.block_number(),
        last_scanned.block_hash(),
        max_blocks_per_range,
        target_extrinsic_hash,
        Some(CancelEventEvidence::Perp(snapshot, identity)),
    )
    .await
}

async fn collect_finalized_recovery_scan_inner(
    endpoints: &DeepXValidatedRpcEndpoints,
    capabilities: &DeepXValidatedRpcMethodCapabilities,
    last_scanned_block: u64,
    last_scanned_block_hash: [u8; 32],
    max_blocks_per_range: u64,
    target_extrinsic_hash: [u8; 32],
    event_evidence: Option<CancelEventEvidence<'_>>,
) -> Result<DeepXFinalizedRecoveryCollection, DeepXTransactionWatchError> {
    let recovery_url = endpoints.url_for(DeepXRpcRole::Recovery);
    let recovery_capabilities = capabilities.for_role(DeepXRpcRole::Recovery);
    if recovery_capabilities.role() != DeepXRpcRole::Recovery
        || recovery_capabilities.endpoint_url() != recovery_url
        || DEEPX_RECOVERY_RPC_METHODS
            .iter()
            .any(|method| !recovery_capabilities.methods().contains(*method))
    {
        return Err(DeepXTransactionWatchError::CapabilitiesMismatch(
            DeepXRpcRole::Recovery,
        ));
    }
    let submission_url = endpoints.url_for(DeepXRpcRole::Submission);
    let submission_capabilities = capabilities.for_role(DeepXRpcRole::Submission);
    if submission_capabilities.role() != DeepXRpcRole::Submission
        || submission_capabilities.endpoint_url() != submission_url
        || DEEPX_SUBMISSION_RPC_METHODS
            .iter()
            .any(|method| !submission_capabilities.methods().contains(*method))
    {
        return Err(DeepXTransactionWatchError::CapabilitiesMismatch(
            DeepXRpcRole::Submission,
        ));
    }

    let client = BlockchainHttpRpcClient::new(recovery_url.to_string(), None, None);
    let encoded_checkpoint_hash: Option<String> = execute_optional_rpc_read_with_retry(
        &client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHash",
            "params": [last_scanned_block],
        }),
    )
    .await
    .map_err(|source| DeepXTransactionWatchError::Rpc {
        method: "chain_getBlockHash",
        source,
    })?;
    let encoded_checkpoint_hash = encoded_checkpoint_hash.ok_or(
        DeepXTransactionWatchError::BlockUnavailable(last_scanned_block),
    )?;
    let observed_checkpoint_hash = decode_hash(&encoded_checkpoint_hash)?;
    if observed_checkpoint_hash != last_scanned_block_hash {
        return Err(DeepXTransactionWatchError::RecoveryCheckpointMismatch {
            block_number: last_scanned_block,
            expected: last_scanned_block_hash,
            received: observed_checkpoint_hash,
        });
    }
    let encoded_finalized_hash: String = execute_rpc_read_with_retry(
        &client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getFinalizedHead",
            "params": [],
        }),
    )
    .await
    .map_err(|source| DeepXTransactionWatchError::Rpc {
        method: "chain_getFinalizedHead",
        source,
    })?;
    let finalized_hash = decode_hash(&encoded_finalized_hash)?;
    let finalized_header: Option<RpcBlockHeader> = execute_optional_rpc_read_with_retry(
        &client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getHeader",
            "params": [encoded_finalized_hash],
        }),
    )
    .await
    .map_err(|source| DeepXTransactionWatchError::Rpc {
        method: "chain_getHeader",
        source,
    })?;
    let finalized_header =
        finalized_header.ok_or(DeepXTransactionWatchError::FinalizedHeaderUnavailable)?;
    let finalized_block_number = decode_block_number(&finalized_header.number)?;
    let checkpoint = DeepXFinalizedRecoveryCheckpoint {
        block_number: finalized_block_number,
        block_hash: finalized_hash,
    };

    let plan = plan_missed_block_scan(
        last_scanned_block,
        finalized_block_number,
        max_blocks_per_range,
    )?;
    let DeepXMissedBlockScanPlan::Scan(ranges) = plan else {
        if finalized_hash != last_scanned_block_hash {
            return Err(DeepXTransactionWatchError::RecoveryCheckpointMismatch {
                block_number: last_scanned_block,
                expected: last_scanned_block_hash,
                received: finalized_hash,
            });
        }
        return Ok(DeepXFinalizedRecoveryCollection::UpToDate(checkpoint));
    };
    let mut collector = DeepXRecoveryScanCollector::new(ranges)
        .ok_or(DeepXRecoveryScanCollectionError::CollectionComplete)?;
    while let Some(range) = collector.next_range() {
        let mut blocks = Vec::new();
        for block_number in range.first_block()..=range.last_block() {
            let observation =
                observe_canonical_block_at(recovery_url, block_number, target_extrinsic_hash)
                    .await?;
            if let Some(extrinsic_index) = observation.extrinsic_index() {
                let Some(ref evidence) = event_evidence else {
                    return Err(DeepXTransactionWatchError::EventEvidenceUnavailable {
                        block_number,
                        extrinsic_index,
                    });
                };
                let event_bytes = fetch_system_events(
                    recovery_url,
                    observation.block_hash(),
                    observation.block_number(),
                )
                .await?;
                let inclusion = match evidence {
                    CancelEventEvidence::PerpClose(snapshot, identity) => {
                        verify_perp_close_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                    CancelEventEvidence::PerpProfitAndLossPoint(snapshot, identity) => {
                        verify_perp_profit_and_loss_point_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                    CancelEventEvidence::PerpModify(snapshot, identity, params) => {
                        verify_perp_modify_inclusion_events(
                            snapshot,
                            identity,
                            *params,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                    CancelEventEvidence::PerpPlace(snapshot, identity) => {
                        verify_perp_place_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                    CancelEventEvidence::Perp(snapshot, identity) => {
                        verify_perp_cancel_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                    CancelEventEvidence::Spot(snapshot, identity) => {
                        verify_spot_cancel_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                    CancelEventEvidence::SpotPlace(snapshot, identity) => {
                        verify_spot_place_inclusion_events(
                            snapshot,
                            identity,
                            observation.block_hash(),
                            observation.block_number(),
                            extrinsic_index,
                            &event_bytes,
                        )?
                    }
                };
                blocks.push(DeepXCanonicalBlockEvidence::new(
                    observation.block_number(),
                    observation.block_hash(),
                    Some(inclusion),
                ));
                continue;
            }
            blocks.push(DeepXCanonicalBlockEvidence::new(
                observation.block_number(),
                observation.block_hash(),
                None,
            ));
        }
        collector.push_range(range, blocks)?;
    }
    let pool = observe_submission_pool(endpoints, target_extrinsic_hash).await?;
    let pool_evidence = match pool {
        DeepXPoolObservation::Present => DeepXSubmissionPoolEvidence::Present,
        DeepXPoolObservation::Absent => DeepXSubmissionPoolEvidence::Unknown,
    };
    let scan = collector.finish(finalized_hash, pool_evidence)?;
    Ok(DeepXFinalizedRecoveryCollection::Scan(scan))
}

async fn fetch_system_events(
    recovery_url: &str,
    block_hash: [u8; 32],
    block_number: u64,
) -> Result<Vec<u8>, DeepXTransactionWatchError> {
    let client = BlockchainHttpRpcClient::new(recovery_url.to_string(), None, None);
    let block_hash = format!("0x{}", hex::encode(block_hash));
    let encoded = fetch_storage_value(&client, SYSTEM_EVENTS_STORAGE_KEY, &block_hash).await?;
    if let Some(encoded) = encoded {
        return decode_event_storage_value(&encoded, block_number);
    }

    let threads_key = system_threads_storage_key(block_number);
    let encoded_threads = fetch_storage_value(&client, &threads_key, &block_hash)
        .await?
        .ok_or(DeepXTransactionWatchError::EventStorageUnavailable(
            block_number,
        ))?;
    let threads = decode_event_storage_value(&encoded_threads, block_number)?;
    let [last_thread] = threads.as_slice() else {
        return Err(DeepXTransactionWatchError::InvalidEventStorage(
            block_number,
        ));
    };
    let mut batches = Vec::with_capacity(usize::from(*last_thread) + 1);
    for thread in 0..=*last_thread {
        let key = system_events_map_storage_key(block_number, thread);
        let encoded = fetch_storage_value(&client, &key, &block_hash)
            .await?
            .ok_or(DeepXTransactionWatchError::EventStorageUnavailable(
                block_number,
            ))?;
        batches.push(decode_event_storage_value(&encoded, block_number)?);
    }
    combine_events_map_batches(&batches, block_number)
}

async fn fetch_storage_value(
    client: &BlockchainHttpRpcClient,
    storage_key: &str,
    block_hash: &str,
) -> Result<Option<String>, DeepXTransactionWatchError> {
    client
        .execute_optional_rpc_call(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "state_getStorage",
            "params": [storage_key, block_hash],
        }))
        .await
        .map_err(|source| DeepXTransactionWatchError::Rpc {
            method: "state_getStorage",
            source,
        })
}

fn decode_event_storage_value(
    encoded: &str,
    block_number: u64,
) -> Result<Vec<u8>, DeepXTransactionWatchError> {
    let value =
        encoded
            .strip_prefix("0x")
            .ok_or(DeepXTransactionWatchError::InvalidEventStorage(
                block_number,
            ))?;
    if value.is_empty() {
        return Err(DeepXTransactionWatchError::InvalidEventStorage(
            block_number,
        ));
    }
    hex::decode(value).map_err(|_| DeepXTransactionWatchError::InvalidEventStorage(block_number))
}

const SYSTEM_THREADS_STORAGE_KEY_PREFIX: &str = "System";
const SYSTEM_THREADS_STORAGE_ITEM: &str = "Threads";
const SYSTEM_EVENTS_MAP_STORAGE_ITEM: &str = "EventsMap";

fn system_map_storage_key(prefix: &str, item: &str, key: &[u8]) -> String {
    let mut encoded = Vec::with_capacity(32 + 16);
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(prefix.as_bytes()));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(item.as_bytes()));
    encoded.extend_from_slice(key);
    format!("0x{}", hex::encode(encoded))
}

fn system_threads_storage_key(block_number: u64) -> String {
    system_map_storage_key(
        SYSTEM_THREADS_STORAGE_KEY_PREFIX,
        SYSTEM_THREADS_STORAGE_ITEM,
        &block_number.to_le_bytes(),
    )
}

fn system_events_map_storage_key(block_number: u64, thread: u8) -> String {
    let block_key = sp_crypto_hashing::blake2_128(&block_number.to_le_bytes());
    let thread_key = sp_crypto_hashing::blake2_128(&[thread]);
    let mut encoded = Vec::with_capacity(64);
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(
        SYSTEM_THREADS_STORAGE_KEY_PREFIX.as_bytes(),
    ));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(
        SYSTEM_EVENTS_MAP_STORAGE_ITEM.as_bytes(),
    ));
    encoded.extend_from_slice(&block_key);
    encoded.extend_from_slice(&thread_key);
    format!("0x{}", hex::encode(encoded))
}

fn combine_events_map_batches(
    batches: &[Vec<u8>],
    block_number: u64,
) -> Result<Vec<u8>, DeepXTransactionWatchError> {
    let mut records = Vec::new();
    for batch in batches {
        let mut input = batch.as_slice();
        let count = Compact::<u32>::decode(&mut input)
            .map_err(|_| DeepXTransactionWatchError::InvalidEventStorage(block_number))?;
        let count = count.0 as usize;
        if input.is_empty() && count != 0 {
            return Err(DeepXTransactionWatchError::InvalidEventStorage(
                block_number,
            ));
        }
        records.extend_from_slice(input);
        if !input.is_empty() && count == 0 {
            return Err(DeepXTransactionWatchError::InvalidEventStorage(
                block_number,
            ));
        }
    }
    let count = batches
        .iter()
        .try_fold(0u32, |total, batch| {
            let mut input = batch.as_slice();
            let count = Compact::<u32>::decode(&mut input).ok()?.0;
            total.checked_add(count)
        })
        .ok_or(DeepXTransactionWatchError::InvalidEventStorage(
            block_number,
        ))?;
    let mut combined = Compact(count).encode();
    combined.extend_from_slice(&records);
    Ok(combined)
}

/// Observes exact transaction membership in the submission endpoint's pending pool.
///
/// `Absent` is returned only after every entry was decoded and hashed successfully. Because the
/// pool response is not atomic with any chain checkpoint, this observation must not be converted
/// directly into authoritative absence or `not-included` evidence.
///
/// # Errors
///
/// Returns an error for RPC failure, malformed pending extrinsics, or duplicate target entries.
pub async fn observe_submission_pool(
    endpoints: &DeepXValidatedRpcEndpoints,
    target_extrinsic_hash: [u8; 32],
) -> Result<DeepXPoolObservation, DeepXTransactionWatchError> {
    let client = BlockchainHttpRpcClient::new(
        endpoints.url_for(DeepXRpcRole::Submission).to_string(),
        None,
        None,
    );
    let extrinsics: Vec<String> = execute_rpc_read_with_retry(
        &client,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "author_pendingExtrinsics",
            "params": [],
        }),
    )
    .await
    .map_err(|source| DeepXTransactionWatchError::Rpc {
        method: "author_pendingExtrinsics",
        source,
    })?;

    Ok(
        if find_extrinsic(&extrinsics, target_extrinsic_hash, "submission pool")?.is_some() {
            DeepXPoolObservation::Present
        } else {
            DeepXPoolObservation::Absent
        },
    )
}

fn find_extrinsic(
    encoded_extrinsics: &[String],
    target_hash: [u8; 32],
    source: &'static str,
) -> Result<Option<u32>, DeepXTransactionWatchError> {
    let mut matched = None;
    for (index, encoded) in encoded_extrinsics.iter().enumerate() {
        let bytes = encoded
            .strip_prefix("0x")
            .ok_or(DeepXTransactionWatchError::InvalidExtrinsic {
                evidence_source: source,
                index,
            })
            .and_then(|value| {
                hex::decode(value).map_err(|_| DeepXTransactionWatchError::InvalidExtrinsic {
                    evidence_source: source,
                    index,
                })
            })?;
        validate_extrinsic(&bytes).map_err(|()| DeepXTransactionWatchError::InvalidExtrinsic {
            evidence_source: source,
            index,
        })?;
        if BlakeTwo256.hash(&bytes).0 == target_hash {
            let index =
                u32::try_from(index).map_err(|_| DeepXTransactionWatchError::InvalidExtrinsic {
                    evidence_source: source,
                    index,
                })?;
            if matched.replace(index).is_some() {
                return Err(DeepXTransactionWatchError::DuplicateExtrinsic {
                    evidence_source: source,
                });
            }
        }
    }
    Ok(matched)
}

fn validate_extrinsic(bytes: &[u8]) -> Result<(), ()> {
    let mut remaining = bytes;
    let payload_length = Compact::<u32>::decode(&mut remaining).map_err(|_| ())?.0;
    usize::try_from(payload_length)
        .ok()
        .filter(|length| *length == remaining.len())
        .map(|_| ())
        .ok_or(())
}

fn decode_hash(encoded: &str) -> Result<[u8; 32], DeepXTransactionWatchError> {
    encoded
        .strip_prefix("0x")
        .ok_or_else(|| DeepXTransactionWatchError::InvalidBlockHash(encoded.to_string()))
        .and_then(|value| {
            hex::decode_array::<32>(value)
                .map_err(|_| DeepXTransactionWatchError::InvalidBlockHash(encoded.to_string()))
        })
}

fn decode_block_number(encoded: &str) -> Result<u64, DeepXTransactionWatchError> {
    let value = encoded
        .strip_prefix("0x")
        .ok_or_else(|| DeepXTransactionWatchError::InvalidBlockNumber(encoded.to_string()))?;
    u64::from_str_radix(value, 16)
        .map_err(|_| DeepXTransactionWatchError::InvalidBlockNumber(encoded.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
    use frame_metadata::{META_RESERVED, RuntimeMetadata, RuntimeMetadataPrefixed};
    use parity_scale_codec::Encode;
    use rstest::rstest;
    use scale_info::{Field, TypeDef, TypeDefPrimitive, Variant};
    use serde_json::Value;
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn historical_runtime_uses_metadata_at_the_requested_block() {
        let block_hash = [0x42; 32];
        let encoded_hash = format!("0x{}", hex::encode(block_hash));
        let metadata: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let router = Router::new().route(
            "/",
            post({
                let calls = Arc::clone(&calls);
                let encoded_hash = encoded_hash.clone();
                move |Json(request): Json<Value>| {
                    let calls = Arc::clone(&calls);
                    let encoded_hash = encoded_hash.clone();
                    let metadata = metadata.clone();
                    async move {
                        assert_eq!(request["params"], json!([encoded_hash]));
                        calls.fetch_add(1, Ordering::Relaxed);
                        let result = match request["method"].as_str().unwrap() {
                            "state_getRuntimeVersion" => json!({
                                "specVersion": 377,
                                "transactionVersion": 2,
                            }),
                            "state_getMetadata" => metadata["result"].clone(),
                            method => panic!("unexpected historical runtime RPC: {method}"),
                        };
                        Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let genesis_hash =
            hex::decode_array("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap();

        let snapshot = observe_historical_read_only_runtime(
            &DeepXEnvironment::Testnet,
            genesis_hash,
            &format!("http://{address}"),
            block_hash,
        )
        .await
        .unwrap();

        assert_eq!(snapshot.identity().spec_version, 377);
        assert_eq!(snapshot.identity().transaction_version, 2);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn rpc_read_retries_transient_transport_failures() {
        async fn rpc(State(attempts): State<Arc<AtomicUsize>>) -> Result<Json<Value>, StatusCode> {
            if attempts.fetch_add(1, Ordering::Relaxed) < 2 {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            Ok(Json(json!({ "jsonrpc": "2.0", "id": 1, "result": 42 })))
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        tokio::spawn({
            let attempts = Arc::clone(&attempts);
            async move {
                axum::serve(
                    listener,
                    Router::new().route("/", post(rpc)).with_state(attempts),
                )
                .await
                .unwrap();
            }
        });
        let client = BlockchainHttpRpcClient::new(format!("http://{address}"), None, None);

        let value: u64 = execute_rpc_read_with_retry(
            &client,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "test", "params": [] }),
        )
        .await
        .unwrap();

        assert_eq!(value, 42);
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[rstest]
    fn events_map_batches_combine_compact_counts() {
        let mut first = Compact(1_u32).encode();
        first.push(10);
        let mut second = Compact(2_u32).encode();
        second.extend_from_slice(&[20, 30]);
        let combined = combine_events_map_batches(&[first, second], 42).unwrap();
        assert_eq!(
            combined,
            [Compact(3_u32).encode(), vec![10, 20, 30]].concat()
        );
    }

    #[rstest]
    fn events_map_batches_reject_nonempty_zero_count() {
        let mut batch = Compact(0_u32).encode();
        batch.push(1);
        assert!(combine_events_map_batches(&[batch], 42).is_err());
    }

    #[rstest]
    fn spec_376_finalized_event_fixture_is_complete_and_read_only_approved() {
        const FIXTURE_ROOT: &str = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/finalized-events-19592700-spec376/",
        );
        let metadata_response: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        let metadata_bytes = hex::decode(
            metadata_response["result"]
                .as_str()
                .unwrap()
                .trim_start_matches("0x"),
        )
        .unwrap();
        let read_only = crate::signing::DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array("87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7")
                .unwrap(),
            376,
            1,
            &metadata_bytes,
        )
        .unwrap();
        let manifest: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/finalized-events-19592700-spec376/manifest.json",
        )))
        .unwrap();
        let initial: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/finalized-events-19592700-spec376/system_events.json",
        )))
        .unwrap();
        let repeated: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/finalized-events-19592700-spec376/system_events_repeated.json",
        )))
        .unwrap();
        let queried: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/finalized-events-19592700-spec376/system_events_query_storage_at.json",
        )))
        .unwrap();
        assert_eq!(
            manifest["rpc_payloads"],
            json!([
                "system_events.json",
                "system_events_repeated.json",
                "system_events_query_storage_at.json",
            ]),
        );
        assert_eq!(initial["result"], repeated["result"]);
        assert_eq!(queried["result"][0]["block"], manifest["block_hash"]);
        assert_eq!(
            queried["result"][0]["changes"][0][0],
            manifest["storage"]["standard_events_key"],
        );
        assert_eq!(queried["result"][0]["changes"][0][1], initial["result"]);
        let event_bytes = hex::decode(
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/test_data/runtime/testnet/finalized-events-19592700-spec376/",
                "reassembled_events.hex",
            ))
            .trim()
            .trim_start_matches("0x"),
        )
        .unwrap();
        assert!(std::path::Path::new(FIXTURE_ROOT).is_dir());
        let mut remaining = event_bytes.as_slice();
        let declared = Compact::<u32>::decode(&mut remaining).unwrap().0;
        let mut consumed = event_bytes.len() - remaining.len();
        let mut decoded = 0_u32;
        let mut inventory = Vec::new();

        for event in Events::<DeepXRuntimeConfig>::decode_from(
            event_bytes.clone(),
            read_only.metadata().clone(),
        )
        .iter()
        {
            let event = event.unwrap();
            decoded += 1;
            consumed += event.bytes().len();
            inventory.push(format!("{}.{}", event.pallet_name(), event.variant_name()));
        }

        assert_eq!(declared, 15);
        assert_eq!(decoded, declared);
        assert_eq!(consumed, event_bytes.len());
        assert_eq!(
            inventory,
            [
                "System.ExtrinsicSuccess",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "System.ExtrinsicFailed",
                "PerpMarket.OrderCancelled",
                "System.ExtrinsicSuccess",
                "PerpMarket.OrderCancelled",
                "System.ExtrinsicSuccess",
                "System.ExtrinsicFailed",
                "PerpMarket.OrderCancelled",
                "System.ExtrinsicSuccess",
            ]
        );
    }
    use nautilus_model::{
        enums::OrderSide,
        identifiers::{ClientOrderId, InstrumentId},
    };

    use crate::{
        common::{
            DeepXEnvironment, DeepXKeyScheme, DeepXPrivateKey, consts::DEEPX_TESTNET_GENESIS_HASH,
        },
        config::{DeepXNetworkConfig, DeepXObservedRpcEndpoint, validate_rpc_endpoint_identities},
        rpc::observe_and_validate_rpc_method_capabilities,
        signing::derive_signer_account_id,
        transaction::{
            DeepXDirectRuntimeIdentity, DeepXInclusionOutcome, DeepXNonceReservation,
            DeepXRecoveryDecision,
        },
    };

    #[derive(Clone)]
    struct RpcState {
        extrinsics: Arc<[String]>,
        block_number: String,
    }

    async fn rpc(State(state): State<RpcState>, Json(request): Json<Value>) -> Json<Value> {
        let result = match request["method"].as_str().unwrap() {
            "chain_getBlockHash" => json!(format!("0x{}", "2a".repeat(32))),
            "chain_getBlock" => json!({
                "block": {
                    "header": { "number": state.block_number },
                    "extrinsics": state.extrinsics.as_ref(),
                },
            }),
            "author_pendingExtrinsics" => json!(state.extrinsics.as_ref()),
            method => panic!("unexpected method {method}"),
        };
        Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
    }

    async fn endpoints(extrinsics: Vec<String>, block_number: &str) -> DeepXValidatedRpcEndpoints {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = RpcState {
            extrinsics: Arc::from(extrinsics),
            block_number: block_number.to_string(),
        };
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/", post(rpc)).with_state(state),
            )
            .await
            .unwrap();
        });
        let url = format!("http://{address}");
        let config = DeepXNetworkConfig {
            base_url_rpc: Some(url.clone()),
            ..Default::default()
        };
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        validate_rpc_endpoint_identities(
            &config,
            [
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Submission, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Recovery, url, genesis_hash),
            ],
        )
        .unwrap()
    }

    fn extrinsic(payload: &[u8]) -> Vec<u8> {
        let mut encoded = Compact(u32::try_from(payload.len()).unwrap()).encode();
        encoded.extend_from_slice(payload);
        encoded
    }

    fn inclusion(
        block_hash: [u8; 32],
        block_number: u64,
        extrinsic_index: u32,
    ) -> DeepXInclusionEvidence {
        DeepXInclusionEvidence::from_indexed_observations(
            block_hash,
            block_number,
            super::super::DeepXIndexedOutcome {
                extrinsic_index,
                outcome: super::super::DeepXDispatchOutcome::Success,
            },
            super::super::DeepXIndexedOutcome {
                extrinsic_index,
                outcome: super::super::DeepXBusinessEventOutcome::Success,
            },
        )
        .unwrap()
    }

    fn read_only_runtime_snapshot() -> DeepXReadOnlyRuntimeSnapshot {
        let metadata: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        DeepXReadOnlyRuntimeSnapshot::approved_testnet(
            &DeepXEnvironment::Testnet,
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap(),
            376,
            1,
            &hex::decode(
                metadata["result"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("0x"),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn runtime_snapshot() -> RuntimeSnapshot {
        RuntimeSnapshot::from_read_only_for_test(read_only_runtime_snapshot())
    }

    #[rstest]
    #[case(0, 0)]
    #[case(u128::MAX, 0)]
    #[case(0, u128::MAX)]
    #[case(u128::MAX, 17)]
    fn perp_profit_and_loss_events_bind_points(#[case] take: u128, #[case] stop: u128) {
        let snapshot = runtime_snapshot();
        let baseline = cancel_identity([0x11; 20], 7, false);
        let identity = DeepXTransactionIdentity::new_perp_profit_and_loss_point(
            ClientOrderId::new(baseline.client_order_id()),
            baseline.signer(),
            baseline.instrument_id(),
            baseline.order_side(),
            baseline.nonce(),
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
            crate::signing::DeepXPerpProfitAndLossPointParams {
                subaccount: [0x11; 20],
                market_id: 100,
                take_profit_point: take,
                stop_loss_point: stop,
            },
        );
        let optional = |point| {
            if point == 0 {
                ScaleValue::unnamed_variant("None", [])
            } else {
                ScaleValue::unnamed_variant("Some", [ScaleValue::u128(point)])
            }
        };
        let position = ScaleValue::named_composite([
            ("market_id", ScaleValue::u128(100)),
            ("is_long", ScaleValue::bool(false)),
            ("base_asset_amount", ScaleValue::u128(1)),
            ("entry_price", ScaleValue::u128(2)),
            ("leverage", ScaleValue::u128(3)),
            ("last_funding_rate", ScaleValue::i128(-4)),
            ("version", ScaleValue::u128(5)),
            ("realized_pnl", ScaleValue::i128(-6)),
            ("funding_payment", ScaleValue::i128(7)),
            ("owner", ScaleValue::from_bytes([0x11; 20])),
            ("take_profit", optional(take)),
            ("stop_loss", optional(stop)),
            ("last_settle_price", ScaleValue::u128(8)),
        ]);
        let fields = ScaleValue::named_composite([
            ("owner", ScaleValue::from_bytes([0x11; 20])),
            ("market_id", ScaleValue::u128(100)),
            ("pos", position),
            ("pnl", ScaleValue::i128(0)),
        ]);
        let pallet = snapshot.metadata().pallet_by_name("PerpMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|e| e.name == "PositionUpdated")
            .unwrap();
        let encode = |fields: &ScaleValue<()>, index| {
            let ValueDef::Composite(Composite::Named(fields)) = &fields.value else {
                panic!();
            };
            let mut record = Phase::ApplyExtrinsic(index).encode();
            record.extend([pallet.index(), event.index]);
            for field in &event.fields {
                let value = &fields
                    .iter()
                    .find(|(name, _)| Some(name) == field.name.as_ref())
                    .unwrap()
                    .1;
                subxt_core::ext::scale_value::scale::encode_as_type(
                    value,
                    field.ty.id,
                    snapshot.metadata().types(),
                    &mut record,
                )
                .unwrap();
            }
            Vec::<[u8; 32]>::new().encode_to(&mut record);
            record
        };
        let verify = |bytes: &[u8]| {
            verify_perp_profit_and_loss_point_inclusion_events_with_runtime(
                &snapshot, &identity, [8; 32], 42, 4, bytes,
            )
        };
        let business = encode(&fields, 4);
        let dispatch = dispatch_event_record(&snapshot, 4, true);
        let valid = system_events(&[business.clone(), dispatch.clone()]);
        assert!(verify(&valid).is_ok());
        assert!(
            verify_perp_profit_and_loss_point_inclusion_events_with_runtime(
                &snapshot, &baseline, [8; 32], 42, 4, &valid
            )
            .is_err()
        );
        assert!(verify(&system_events(std::slice::from_ref(&business))).is_err());
        assert!(
            verify(&system_events(&[
                business.clone(),
                dispatch.clone(),
                dispatch.clone()
            ]))
            .is_err()
        );
        let mut wrong_count = valid.clone();
        wrong_count[0] = Compact(3_u32).encode()[0];
        assert!(verify(&wrong_count).is_err());
        assert!(
            verify(&system_events(&[
                business.clone(),
                business,
                dispatch.clone()
            ]))
            .is_err()
        );
        assert!(verify(&system_events(&[encode(&fields, 5), dispatch.clone()])).is_err());
        assert!(verify(&system_events(std::slice::from_ref(&dispatch))).is_err());
        assert!(
            verify(&system_events(&[dispatch_event_record(
                &snapshot, 4, false
            )]))
            .is_ok()
        );
        assert!(verify(&valid[..valid.len() - 1]).is_err());
        let mut trailing = valid;
        trailing.push(0);
        assert!(verify(&trailing).is_err());
        for path in [
            "owner",
            "market_id",
            "pnl",
            "pos.owner",
            "pos.market_id",
            "pos.take_profit",
            "pos.stop_loss",
        ] {
            let mut changed = fields.clone();
            let replacement = match path {
                "owner" | "pos.owner" => ScaleValue::from_bytes([0x22; 20]),
                "market_id" | "pos.market_id" => ScaleValue::u128(101),
                "pnl" => ScaleValue::i128(1),
                _ => ScaleValue::unnamed_variant("Some", [ScaleValue::u128(1)]),
            };
            let ValueDef::Composite(Composite::Named(outer)) = &mut changed.value else {
                panic!();
            };
            if let Some(name) = path.strip_prefix("pos.") {
                let pos = &mut outer.iter_mut().find(|(name, _)| name == "pos").unwrap().1;
                let ValueDef::Composite(Composite::Named(inner)) = &mut pos.value else {
                    panic!();
                };
                inner.iter_mut().find(|(key, _)| key == name).unwrap().1 = replacement;
            } else {
                outer.iter_mut().find(|(key, _)| key == path).unwrap().1 = replacement;
            }
            assert!(
                verify(&system_events(&[encode(&changed, 4), dispatch.clone()])).is_err(),
                "{path}"
            );
        }
    }

    #[rstest]
    #[case(0, None)]
    #[case(0, Some(u64::MAX))]
    #[case(u128::MAX, Some(19))]
    fn perp_close_events_bind_generated_order(#[case] price: u128, #[case] slippage: Option<u64>) {
        let snapshot = runtime_snapshot();
        let baseline = perp_place_identity(true, price == 0);
        let close = DeepXTransactionIdentity::new_perp_close(
            ClientOrderId::new(baseline.client_order_id()),
            baseline.signer(),
            baseline.instrument_id(),
            OrderSide::Sell,
            baseline.nonce(),
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
            crate::signing::DeepXPerpCloseParams {
                subaccount: [0x11; 20],
                market_id: 100,
                price,
                slippage,
            },
        );
        let generated = DeepXTransactionIdentity::new_perp_close(
            ClientOrderId::new(baseline.client_order_id()),
            baseline.signer(),
            baseline.instrument_id(),
            OrderSide::Sell,
            DeepXNonceReservation::TimestampOrderId { value: 19 },
            baseline.runtime().clone(),
            crate::signing::DeepXPerpCloseParams {
                subaccount: [0x11; 20],
                market_id: 100,
                price,
                slippage,
            },
        );
        assert_ne!(generated.nonce(), close.nonce());
        let mut order = perp_place_order_value(&baseline, 42);
        let ValueDef::Composite(Composite::Named(fields)) = &mut order.value else {
            panic!();
        };
        let mut set = |name, replacement| {
            fields.iter_mut().find(|(key, _)| key == name).unwrap().1 = replacement;
        };
        set("order_id", ScaleValue::u128(19));
        set("reduce_only", ScaleValue::bool(true));
        set("take_profit", ScaleValue::unnamed_variant("None", []));
        set("stop_loss", ScaleValue::unnamed_variant("None", []));
        set(
            "order_type",
            if price == 0 {
                ScaleValue::unnamed_variant(
                    "Market",
                    [match slippage {
                        None => ScaleValue::unnamed_variant("None", []),
                        Some(value) => ScaleValue::unnamed_variant(
                            "Some",
                            [ScaleValue::u128(u128::from(value))],
                        ),
                    }],
                )
            } else {
                ScaleValue::unnamed_variant("Stop", [])
            },
        );
        let business =
            |order: &ScaleValue<()>| perp_place_event_record(&snapshot, 4, &generated, order);
        let dispatch = dispatch_event_record(&snapshot, 4, true);
        let verify = |events: &[u8]| {
            verify_perp_close_inclusion_events_with_runtime(
                &snapshot, &close, [8; 32], 42, 4, events,
            )
        };
        let valid = system_events(&[business(&order), dispatch.clone()]);
        assert!(verify(&valid).is_ok());
        assert!(verify(&system_events(&[business(&order)])).is_err());
        assert!(
            verify(&system_events(&[
                perp_place_event_record(&snapshot, 5, &generated, &order),
                dispatch.clone()
            ]))
            .is_err()
        );
        if price != 0 {
            let mut changed = order.clone();
            let ValueDef::Composite(Composite::Named(fields)) = &mut changed.value else {
                panic!();
            };
            fields
                .iter_mut()
                .find(|(name, _)| name == "price")
                .unwrap()
                .1 = ScaleValue::u128(1);
            assert!(verify(&system_events(&[business(&changed), dispatch.clone()])).is_err());
        }
        assert!(
            verify(&system_events(&[
                business(&order),
                business(&order),
                dispatch.clone()
            ]))
            .is_err()
        );
        assert!(verify(&system_events(std::slice::from_ref(&dispatch))).is_err());
        assert!(
            verify(&system_events(&[dispatch_event_record(
                &snapshot, 4, false
            )]))
            .is_ok()
        );
        assert!(verify(&valid[..valid.len() - 1]).is_err());
        let mut trailing = valid;
        trailing.push(0);
        assert!(verify(&trailing).is_err());
        for (name, replacement) in [
            ("order_id", ScaleValue::u128(20)),
            ("owner", ScaleValue::from_bytes([0x22; 20])),
            ("market_id", ScaleValue::u128(101)),
            ("reduce_only", ScaleValue::bool(false)),
            (
                "take_profit",
                ScaleValue::unnamed_variant("Some", [ScaleValue::u128(1)]),
            ),
            (
                "stop_loss",
                ScaleValue::unnamed_variant("Some", [ScaleValue::u128(1)]),
            ),
            ("post_only", ScaleValue::unnamed_variant("MustPostOnly", [])),
            (
                "order_type",
                ScaleValue::unnamed_variant(
                    "Market",
                    [ScaleValue::unnamed_variant("Some", [ScaleValue::u128(1)])],
                ),
            ),
            ("size_filled", ScaleValue::u128(1)),
            ("size_remain", ScaleValue::u128(1)),
            ("create_time", ScaleValue::u128(41)),
            ("status", ScaleValue::unnamed_variant("Filled", [])),
        ] {
            let mut changed = order.clone();
            let ValueDef::Composite(Composite::Named(fields)) = &mut changed.value else {
                panic!();
            };
            fields.iter_mut().find(|(key, _)| key == name).unwrap().1 = replacement;
            assert!(
                verify(&system_events(&[business(&changed), dispatch.clone()])).is_err(),
                "{name}"
            );
        }
    }

    fn perp_place_identity(is_long: bool, market: bool) -> DeepXTransactionIdentity {
        let baseline = cancel_identity([0x11; 20], 7, false);
        DeepXTransactionIdentity::new_perp_place(
            ClientOrderId::new(baseline.client_order_id()),
            baseline.signer(),
            baseline.instrument_id(),
            if is_long {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            baseline.nonce(),
            baseline.runtime().clone(),
            crate::signing::DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 100,
                is_long,
                size: u128::MAX,
                price: if market { 0 } else { u128::MAX },
                order_type: if market {
                    crate::signing::DeepXPerpOrderType::Market(Some(0))
                } else {
                    crate::signing::DeepXPerpOrderType::Limit(crate::signing::DeepXTimeInForce::Gtc)
                },
                take_profit: Some(u128::MAX),
                stop_loss: Some(0),
                reduce_only: false,
                post_only: crate::signing::DeepXPostOnlyParam::None,
            },
        )
    }

    fn perp_place_order_value(
        identity: &DeepXTransactionIdentity,
        block_number: u64,
    ) -> ScaleValue<()> {
        let Some(DeepXTransactionOperation::PerpPlace {
            subaccount,
            market_id,
            is_long,
            size,
            price,
            order_type,
            take_profit,
            stop_loss,
            reduce_only,
            post_only,
        }) = identity.operation()
        else {
            unreachable!();
        };
        let DeepXNonceReservation::TimestampOrderId { value: order_id } = identity.nonce() else {
            unreachable!();
        };
        let optional = |point: Option<u128>| match point {
            Some(value) => ScaleValue::unnamed_variant("Some", [ScaleValue::u128(value)]),
            None => ScaleValue::unnamed_variant("None", []),
        };
        let order_type_value = match order_type {
            crate::signing::DeepXPerpOrderType::Limit(tif) => ScaleValue::unnamed_variant(
                "Limit",
                [ScaleValue::unnamed_variant(
                    match tif {
                        crate::signing::DeepXTimeInForce::Gtc => "GTC",
                        crate::signing::DeepXTimeInForce::Ioc => "IOC",
                        crate::signing::DeepXTimeInForce::Fok => "FOK",
                    },
                    [],
                )],
            ),
            crate::signing::DeepXPerpOrderType::Market(slippage) => {
                ScaleValue::unnamed_variant("Market", [optional(slippage.map(u128::from))])
            }
            crate::signing::DeepXPerpOrderType::Stop => ScaleValue::unnamed_variant("Stop", []),
        };
        let post_only = match post_only {
            crate::signing::DeepXPostOnlyParam::None => "None",
            crate::signing::DeepXPostOnlyParam::MustPostOnly => "MustPostOnly",
            crate::signing::DeepXPostOnlyParam::Adaptive => "Adaptive",
        };
        ScaleValue::named_composite([
            ("order_id", ScaleValue::u128(u128::from(order_id))),
            ("owner", ScaleValue::from_bytes(subaccount)),
            ("market_id", ScaleValue::u128(u128::from(*market_id))),
            ("is_long", ScaleValue::bool(*is_long)),
            ("size", ScaleValue::u128(*size)),
            (
                "price",
                ScaleValue::u128(
                    if matches!(order_type, crate::signing::DeepXPerpOrderType::Market(_)) {
                        123
                    } else {
                        *price
                    },
                ),
            ),
            ("order_type", order_type_value),
            ("create_time", ScaleValue::u128(u128::from(block_number))),
            ("leverage", ScaleValue::u128(25000)),
            ("status", ScaleValue::unnamed_variant("Open", [])),
            ("size_filled", ScaleValue::u128(0)),
            ("size_remain", ScaleValue::u128(*size)),
            ("take_profit", optional(*take_profit)),
            ("stop_loss", optional(*stop_loss)),
            ("reduce_only", ScaleValue::bool(*reduce_only)),
            ("post_only", ScaleValue::unnamed_variant(post_only, [])),
        ])
    }

    fn perp_place_event_record(
        snapshot: &impl RuntimeMetadataView,
        index: u32,
        identity: &DeepXTransactionIdentity,
        order: &ScaleValue<()>,
    ) -> Vec<u8> {
        let pallet = snapshot.metadata().pallet_by_name("PerpMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == "OrderPlaced")
            .unwrap();
        let DeepXNonceReservation::TimestampOrderId { value: order_id } = identity.nonce() else {
            unreachable!();
        };
        let mut record = Phase::ApplyExtrinsic(index).encode();
        record.extend([pallet.index(), event.index]);
        for field in &event.fields {
            let value = match field.name.as_deref() {
                Some("order_id") => ScaleValue::u128(u128::from(order_id)),
                Some("order") => order.clone(),
                _ => panic!("unexpected placement field"),
            };
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        record
    }

    fn perp_modify_params() -> DeepXPerpModifyParams {
        DeepXPerpModifyParams {
            cancel: crate::signing::DeepXPerpCancelParams {
                subaccount: [0x11; 20],
                order_id: 7,
                market_id: 100,
                fast_cancel: false,
            },
            place: crate::signing::DeepXPerpPlaceParams {
                subaccount: [0x11; 20],
                market_id: 100,
                is_long: true,
                size: u128::MAX,
                price: u128::MAX,
                order_type: crate::signing::DeepXPerpOrderType::Limit(
                    crate::signing::DeepXTimeInForce::Gtc,
                ),
                take_profit: Some(u128::MAX),
                stop_loss: Some(0),
                reduce_only: false,
                post_only: crate::signing::DeepXPostOnlyParam::None,
            },
        }
    }

    fn perp_modify_base_identity() -> DeepXTransactionIdentity {
        let existing = cancel_identity([0x11; 20], 7, false);
        DeepXTransactionIdentity::new_perp_modify(
            ClientOrderId::new(existing.client_order_id()),
            existing.signer(),
            existing.instrument_id(),
            OrderSide::Buy,
            existing.nonce(),
            existing.runtime().clone(),
            perp_modify_params(),
        )
    }

    fn perp_modify_event_records(
        snapshot: &impl RuntimeMetadataView,
        index: u32,
        block_number: u64,
        params: DeepXPerpModifyParams,
    ) -> Vec<Vec<u8>> {
        let base = perp_modify_base_identity();
        let place_identity = DeepXTransactionIdentity::new_perp_place(
            ClientOrderId::new(base.client_order_id()),
            base.signer(),
            base.instrument_id(),
            OrderSide::Buy,
            base.nonce(),
            base.runtime().clone(),
            params.place,
        );
        vec![
            cancel_event_record(
                snapshot,
                index,
                params.cancel.subaccount,
                params.cancel.order_id,
                0,
            ),
            perp_place_event_record(
                snapshot,
                index,
                &place_identity,
                &perp_place_order_value(&place_identity, block_number),
            ),
            dispatch_event_record(snapshot, index, true),
        ]
    }

    fn synthetic_perp_modify_metadata() -> subxt_core::metadata::Metadata {
        #[derive(Deserialize)]
        struct MetadataResponse {
            result: String,
        }

        let response: MetadataResponse = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_data/runtime/testnet/",
            "genesis-87b3c185_metadata-57f1bedf_spec-376_tx-1_finalized-e38016d2/metadata.json",
        )))
        .unwrap();
        let bytes = hex::decode(response.result.trim_start_matches("0x")).unwrap();
        let RuntimeMetadataPrefixed(_, RuntimeMetadata::V14(mut metadata)) =
            RuntimeMetadataPrefixed::decode(&mut bytes.as_slice()).unwrap()
        else {
            panic!("Expected V14 fixture metadata");
        };

        let event_type_id = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.name == "Subaccount")
            .and_then(|pallet| pallet.event.as_ref())
            .unwrap()
            .ty
            .id;
        let cancel_type = metadata
            .pallets
            .iter()
            .find(|pallet| pallet.name == "PerpMarket")
            .and_then(|pallet| pallet.event.as_ref())
            .and_then(|event| metadata.types.resolve(event.ty.id))
            .and_then(|ty| match &ty.type_def {
                TypeDef::Variant(variants) => variants
                    .variants
                    .iter()
                    .find(|variant| variant.name == "OrderCancelled"),
                _ => None,
            })
            .and_then(|variant| {
                variant
                    .fields
                    .iter()
                    .find(|field| field.name.as_deref() == Some("user"))
            })
            .unwrap()
            .ty
            .id;
        let primitive_id = |primitive| {
            metadata
                .types
                .types
                .iter()
                .find_map(|ty| {
                    matches!(
                        &ty.ty.type_def,
                        TypeDef::Primitive(candidate) if *candidate == primitive
                    )
                    .then_some(ty.id)
                })
                .unwrap()
        };
        let u64_id = primitive_id(TypeDefPrimitive::U64);
        let u128_id = primitive_id(TypeDefPrimitive::U128);
        let fields = vec![
            Field::new(
                Some("subaccount".into()),
                cancel_type.into(),
                None,
                Vec::new(),
            ),
            Field::new(Some("old_order_id".into()), u64_id.into(), None, Vec::new()),
            Field::new(Some("filled_size".into()), u128_id.into(), None, Vec::new()),
            Field::new(Some("target_size".into()), u128_id.into(), None, Vec::new()),
            Field::new(
                Some("replacement_size".into()),
                u128_id.into(),
                None,
                Vec::new(),
            ),
            Field::new(Some("new_order_id".into()), u64_id.into(), None, Vec::new()),
        ];
        let event_type = metadata
            .types
            .types
            .iter_mut()
            .find(|ty| ty.id == event_type_id)
            .unwrap();
        let TypeDef::Variant(variants) = &mut event_type.ty.type_def else {
            panic!("Expected Subaccount event variants");
        };
        let event_index = (0..=u8::MAX)
            .find(|index| {
                !variants
                    .variants
                    .iter()
                    .any(|variant| variant.index == *index)
            })
            .unwrap();
        variants.variants.push(Variant::new(
            "PerpOrderModified".into(),
            fields,
            event_index,
            Vec::new(),
        ));

        let bytes = RuntimeMetadataPrefixed(META_RESERVED, RuntimeMetadata::V14(metadata)).encode();
        subxt_core::metadata::decode_from(&bytes).unwrap()
    }

    fn perp_modify_sizing_event_record(
        metadata: &subxt_core::metadata::Metadata,
        index: u32,
        target_size: u128,
        filled_size: u128,
        replacement_size: u128,
        new_order_id: u64,
    ) -> Vec<u8> {
        let pallet = metadata.pallet_by_name("Subaccount").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == "PerpOrderModified")
            .unwrap();
        let values = ScaleValue::named_composite([
            ("subaccount", ScaleValue::from_bytes([0x11; 20])),
            ("old_order_id", ScaleValue::u128(7)),
            ("filled_size", ScaleValue::u128(filled_size)),
            ("target_size", ScaleValue::u128(target_size)),
            ("replacement_size", ScaleValue::u128(replacement_size)),
            ("new_order_id", ScaleValue::u128(u128::from(new_order_id))),
        ]);
        let ValueDef::Composite(Composite::Named(fields)) = &values.value else {
            unreachable!();
        };
        let mut record = Phase::ApplyExtrinsic(index).encode();
        record.extend([pallet.index(), event.index]);
        for field in &event.fields {
            let value = fields
                .iter()
                .find_map(|(name, value)| (Some(name) == field.name.as_ref()).then_some(value))
                .unwrap();
            subxt_core::ext::scale_value::scale::encode_as_type(
                value,
                field.ty.id,
                metadata.types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        system_events(&[record])
    }

    #[rstest]
    fn perp_modify_sizing_event_decodes_and_binds_exact_sizes() {
        let metadata = synthetic_perp_modify_metadata();
        assert!(perp_modify_sizing_event_supported(&metadata));
        let mut params = perp_modify_params();
        params.place.size = 13;
        let base = cancel_identity([0x11; 20], 7, false);
        let identity = DeepXTransactionIdentity::new_perp_modify(
            ClientOrderId::new(base.client_order_id()),
            base.signer(),
            base.instrument_id(),
            OrderSide::Buy,
            base.nonce(),
            base.runtime().clone(),
            params,
        );
        let DeepXNonceReservation::TimestampOrderId {
            value: new_order_id,
        } = identity.nonce()
        else {
            unreachable!();
        };
        let valid = perp_modify_sizing_event_record(&metadata, 4, 13, 5, 8, new_order_id);

        assert_eq!(
            verify_perp_modify_sizing_event(&metadata, &identity, 4, &valid).unwrap(),
            8,
        );

        let inconsistent = perp_modify_sizing_event_record(&metadata, 4, 13, 5, 9, new_order_id);
        assert!(matches!(
            verify_perp_modify_sizing_event(&metadata, &identity, 4, &inconsistent),
            Err(DeepXPerpModifyEventVerificationError::ConflictingEvidence),
        ));
    }

    #[rstest]
    fn perp_modify_inclusion_requires_runtime_sizing_event() {
        let snapshot = runtime_snapshot();
        let identity = perp_modify_base_identity();
        let params = perp_modify_params();
        let events = system_events(&perp_modify_event_records(&snapshot, 4, 42, params));

        assert!(matches!(
            verify_perp_modify_inclusion_events_with_runtime(
                &snapshot, &identity, params, [8; 32], 42, 4, &events,
            ),
            Err(DeepXPerpModifyEventVerificationError::UnsupportedOperation),
        ));
    }

    #[rstest]
    fn perp_modify_successful_dispatch_without_cancel_event_is_rejected() {
        let snapshot = runtime_snapshot();
        let identity = perp_modify_base_identity();
        let params = perp_modify_params();
        let records = perp_modify_event_records(&snapshot, 4, 42, params);
        let events = system_events(&records[1..]);

        assert!(matches!(
            verify_perp_modify_inclusion_events_with_runtime(
                &snapshot, &identity, params, [8; 32], 42, 4, &events,
            ),
            Err(DeepXPerpModifyEventVerificationError::Cancel(_)),
        ));
    }

    #[rstest]
    fn perp_modify_without_runtime_sizing_event_cannot_verify_leg_order() {
        let snapshot = runtime_snapshot();
        let identity = perp_modify_base_identity();
        let params = perp_modify_params();
        let mut records = perp_modify_event_records(&snapshot, 4, 42, params);
        records.swap(0, 1);
        let events = system_events(&records);

        assert!(matches!(
            verify_perp_modify_inclusion_events_with_runtime(
                &snapshot, &identity, params, [8; 32], 42, 4, &events,
            ),
            Err(DeepXPerpModifyEventVerificationError::UnsupportedOperation),
        ));
    }

    #[rstest]
    #[case(true, false)]
    #[case(false, false)]
    #[case(true, true)]
    #[case(false, true)]
    fn perp_place_events_bind_exact_inputs(#[case] is_long: bool, #[case] market: bool) {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(is_long, market);
        let events = system_events(&[
            perp_place_event_record(
                &snapshot,
                4,
                &identity,
                &perp_place_order_value(&identity, 42),
            ),
            dispatch_event_record(&snapshot, 4, true),
        ]);
        let evidence = verify_perp_place_inclusion_events_with_runtime(
            &snapshot, &identity, [8; 32], 42, 4, &events,
        )
        .unwrap();
        assert_eq!(
            evidence.outcome,
            super::super::DeepXInclusionOutcome::Success
        );
        assert_eq!(evidence.extrinsic_index, 4);
    }

    #[rstest]
    #[case("order_id", ScaleValue::u128(1))]
    #[case("owner", ScaleValue::from_bytes([0x22; 20]))]
    #[case("market_id", ScaleValue::u128(1))]
    #[case("is_long", ScaleValue::bool(false))]
    #[case("size", ScaleValue::u128(1))]
    #[case("price", ScaleValue::u128(1))]
    #[case("take_profit", ScaleValue::unnamed_variant("None", []))]
    #[case("stop_loss", ScaleValue::unnamed_variant("None", []))]
    #[case("reduce_only", ScaleValue::bool(true))]
    #[case("post_only", ScaleValue::unnamed_variant("MustPostOnly", []))]
    #[case("create_time", ScaleValue::u128(43))]
    #[case("size_filled", ScaleValue::u128(1))]
    #[case("size_remain", ScaleValue::u128(1))]
    #[case("status", ScaleValue::unnamed_variant("Filled", []))]
    #[case("order_type", ScaleValue::unnamed_variant("Limit", [ScaleValue::unnamed_variant("IOC", [])]))]
    fn perp_place_conflicting_order_fields_fail(#[case] name: &str, #[case] value: ScaleValue<()>) {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(true, false);
        let mut order = perp_place_order_value(&identity, 42);
        let ValueDef::Composite(Composite::Named(fields)) = &mut order.value else {
            unreachable!();
        };
        *fields
            .iter_mut()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value)
            .unwrap() = value;
        let events = system_events(&[
            perp_place_event_record(&snapshot, 4, &identity, &order),
            dispatch_event_record(&snapshot, 4, true),
        ]);
        assert!(matches!(
            verify_perp_place_inclusion_events_with_runtime(
                &snapshot, &identity, [8; 32], 42, 4, &events,
            ),
            Err(DeepXPerpPlaceEventVerificationError::ConflictingEvent)
        ));
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    fn perp_place_missing_duplicate_or_malformed_events_fail(#[case] mutation: u8) {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(true, false);
        let order = perp_place_event_record(
            &snapshot,
            4,
            &identity,
            &perp_place_order_value(&identity, 42),
        );
        let dispatch = dispatch_event_record(&snapshot, 4, true);
        let mut events = match mutation {
            0 => system_events(&[dispatch]),
            1 => system_events(&[order.clone(), order, dispatch]),
            2 => system_events(&[order, dispatch.clone(), dispatch]),
            _ => system_events(&[order, dispatch]),
        };
        if mutation == 3 {
            events.pop();
        }
        if mutation == 4 {
            events.push(0);
        }
        assert!(
            verify_perp_place_inclusion_events_with_runtime(
                &snapshot, &identity, [8; 32], 42, 4, &events,
            )
            .is_err()
        );
    }

    #[rstest]
    fn perp_place_failed_dispatch_needs_no_placement_event() {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(true, false);
        let events = system_events(&[dispatch_event_record(&snapshot, 4, false)]);
        let evidence = verify_perp_place_inclusion_events_with_runtime(
            &snapshot, &identity, [8; 32], 42, 4, &events,
        )
        .unwrap();
        assert_eq!(
            evidence.outcome,
            super::super::DeepXInclusionOutcome::Failed
        );
    }

    #[rstest]
    #[case(5, 4)]
    #[case(4, 5)]
    fn perp_place_events_at_other_indices_cannot_prove_inclusion(
        #[case] order_index: u32,
        #[case] dispatch_index: u32,
    ) {
        let snapshot = runtime_snapshot();
        let identity = perp_place_identity(true, false);
        let events = system_events(&[
            perp_place_event_record(
                &snapshot,
                order_index,
                &identity,
                &perp_place_order_value(&identity, 42),
            ),
            dispatch_event_record(&snapshot, dispatch_index, true),
        ]);
        assert!(
            verify_perp_place_inclusion_events_with_runtime(
                &snapshot, &identity, [8; 32], 42, 4, &events,
            )
            .is_err()
        );
    }

    fn cancel_identity(
        subaccount: [u8; 20],
        order_id: u64,
        fast_cancel: bool,
    ) -> DeepXTransactionIdentity {
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let snapshot = runtime_snapshot();
        DeepXTransactionIdentity::new_perp_cancel(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            derive_signer_account_id(&key).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId {
                value: 1_725_000_000_125,
            },
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
            crate::signing::DeepXPerpCancelParams {
                subaccount,
                order_id,
                market_id: 100,
                fast_cancel,
            },
        )
    }

    fn spot_place_identity(is_buy: bool) -> DeepXTransactionIdentity {
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let snapshot = runtime_snapshot();
        DeepXTransactionIdentity::new_spot_place(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            derive_signer_account_id(&key).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC.DEEPX").unwrap(),
            if is_buy {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            DeepXNonceReservation::TimestampOrderId {
                value: 1_725_000_000_125,
            },
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
            crate::signing::DeepXSpotPlaceParams {
                subaccount: [0x11; 20],
                pair: [0xa5; 32],
                is_buy,
                quote_amount: std::array::from_fn(|index| index as u8),
                base_amount: std::array::from_fn(|index| (31 - index) as u8),
                order_type: crate::signing::DeepXSpotOrderType::Limit(
                    crate::signing::DeepXTimeInForce::Gtc,
                ),
                post_only: crate::signing::DeepXPostOnlyParam::MustPostOnly,
                reduce_only: false,
            },
        )
    }

    fn u256_value(bytes: [u8; 32]) -> ScaleValue<()> {
        ScaleValue::unnamed_composite([ScaleValue::unnamed_composite(
            bytes
                .as_chunks::<8>()
                .0
                .iter()
                .map(|chunk| ScaleValue::u128(u128::from(u64::from_le_bytes(*chunk)))),
        )])
    }

    fn spot_place_event_record(
        snapshot: &impl RuntimeMetadataView,
        extrinsic_index: u32,
        identity: &DeepXTransactionIdentity,
    ) -> Vec<u8> {
        let Some(DeepXTransactionOperation::SpotPlace {
            subaccount,
            pair,
            is_buy,
            quote_amount,
            base_amount,
            order_type,
            post_only,
            reduce_only,
        }) = identity.operation()
        else {
            unreachable!()
        };
        let DeepXNonceReservation::TimestampOrderId { value: order_id } = identity.nonce() else {
            unreachable!()
        };
        let pallet = snapshot.metadata().pallet_by_name("SpotMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| {
                event.name
                    == if *is_buy {
                        "StateOrderBuy"
                    } else {
                        "StateOrderSell"
                    }
            })
            .unwrap();
        assert_eq!(event.fields.len(), 1);
        assert_eq!(event.fields[0].name.as_deref(), Some("order"));
        let order_type = match order_type {
            crate::signing::DeepXSpotOrderType::Limit(time_in_force) => {
                let name = match time_in_force {
                    crate::signing::DeepXTimeInForce::Gtc => "GTC",
                    crate::signing::DeepXTimeInForce::Ioc => "IOC",
                    crate::signing::DeepXTimeInForce::Fok => "FOK",
                };
                ScaleValue::unnamed_variant("Limit", [ScaleValue::unnamed_variant(name, [])])
            }
            crate::signing::DeepXSpotOrderType::Market(slippage) => {
                let value = match slippage {
                    Some(value) => {
                        ScaleValue::unnamed_variant("Some", [ScaleValue::u128(u128::from(*value))])
                    }
                    None => ScaleValue::unnamed_variant("None", []),
                };
                ScaleValue::unnamed_variant("Market", [value])
            }
            crate::signing::DeepXSpotOrderType::Stop => ScaleValue::unnamed_variant("Stop", []),
        };
        let post_only = match post_only {
            crate::signing::DeepXPostOnlyParam::None => "None",
            crate::signing::DeepXPostOnlyParam::MustPostOnly => "MustPostOnly",
            crate::signing::DeepXPostOnlyParam::Adaptive => "Adaptive",
        };
        let order = ScaleValue::named_composite([
            ("id", ScaleValue::u128(u128::from(order_id))),
            ("maker", ScaleValue::from_bytes(subaccount)),
            ("pair", ScaleValue::from_bytes(pair)),
            (
                "price",
                ScaleValue::unnamed_variant("Some", [u256_value([1; 32])]),
            ),
            ("quote_amount", u256_value(*quote_amount)),
            ("base_amount", u256_value(*base_amount)),
            ("create_time", ScaleValue::u128(42)),
            ("status", ScaleValue::unnamed_variant("Open", [])),
            ("order_type", order_type),
            ("post_only", ScaleValue::unnamed_variant(post_only, [])),
            ("reduce_only", ScaleValue::bool(*reduce_only)),
            ("is_buy", ScaleValue::bool(*is_buy)),
        ]);
        let mut record = Phase::ApplyExtrinsic(extrinsic_index).encode();
        record.extend([pallet.index(), event.index]);
        subxt_core::ext::scale_value::scale::encode_as_type(
            &order,
            event.fields[0].ty.id,
            snapshot.metadata().types(),
            &mut record,
        )
        .unwrap();
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        record
    }

    fn cancel_event_record(
        snapshot: &impl RuntimeMetadataView,
        extrinsic_index: u32,
        subaccount: [u8; 20],
        order_id: u64,
        reason_index: u8,
    ) -> Vec<u8> {
        cancel_event_record_with_phase(
            snapshot,
            Phase::ApplyExtrinsic(extrinsic_index),
            subaccount,
            order_id,
            reason_index,
        )
    }

    fn cancel_event_record_with_phase(
        snapshot: &impl RuntimeMetadataView,
        phase: Phase,
        subaccount: [u8; 20],
        order_id: u64,
        reason_index: u8,
    ) -> Vec<u8> {
        let pallet = snapshot.interfaces().pallet("PerpMarket").unwrap();
        let event = snapshot
            .interfaces()
            .event("PerpMarket", "OrderCancelled")
            .unwrap();
        let mut bytes = phase.encode();
        bytes.push(pallet.index());
        bytes.push(event.index());
        bytes.extend_from_slice(&subaccount);
        order_id.encode_to(&mut bytes);
        bytes.push(reason_index);
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    fn dispatch_event_record(
        snapshot: &impl RuntimeMetadataView,
        extrinsic_index: u32,
        success: bool,
    ) -> Vec<u8> {
        let pallet = snapshot.interfaces().pallet("System").unwrap();
        let event = snapshot
            .interfaces()
            .event(
                "System",
                if success {
                    "ExtrinsicSuccess"
                } else {
                    "ExtrinsicFailed"
                },
            )
            .unwrap();
        let mut bytes = Phase::ApplyExtrinsic(extrinsic_index).encode();
        bytes.push(pallet.index());
        bytes.push(event.index());
        let dispatch_info = ScaleValue::named_composite([
            (
                "weight",
                ScaleValue::named_composite([
                    ("ref_time", ScaleValue::u128(0)),
                    ("proof_size", ScaleValue::u128(0)),
                ]),
            ),
            (
                "call_type",
                ScaleValue::unnamed_variant("Timestamp", [ScaleValue::u128(0)]),
            ),
            (
                "priority",
                ScaleValue::unnamed_composite([ScaleValue::u128(0), ScaleValue::u128(0)]),
            ),
            ("class", ScaleValue::unnamed_variant("Normal", [])),
            ("pays_fee", ScaleValue::unnamed_variant("Yes", [])),
        ]);
        let metadata_event = snapshot
            .metadata()
            .pallet_by_name("System")
            .unwrap()
            .event_variant_by_index(event.index())
            .unwrap();
        if success {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &dispatch_info,
                metadata_event.fields[0].ty.id,
                snapshot.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        } else {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &ScaleValue::unnamed_variant("Other", []),
                metadata_event.fields[0].ty.id,
                snapshot.metadata().types(),
                &mut bytes,
            )
            .unwrap();
            subxt_core::ext::scale_value::scale::encode_as_type(
                &dispatch_info,
                metadata_event.fields[1].ty.id,
                snapshot.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    fn system_events(records: &[Vec<u8>]) -> Vec<u8> {
        let mut bytes = Compact(u32::try_from(records.len()).unwrap()).encode();
        for record in records {
            bytes.extend_from_slice(record);
        }
        bytes
    }

    fn delegate_account_event_record(
        snapshot: &impl RuntimeMetadataView,
        phase: Phase,
        kind: DeepXDelegateAccountChangeKind,
    ) -> Vec<u8> {
        delegate_account_event_record_for_owner(snapshot, phase, kind, [0x11; 20])
    }

    fn delegate_account_event_record_for_owner(
        snapshot: &impl RuntimeMetadataView,
        phase: Phase,
        kind: DeepXDelegateAccountChangeKind,
        owner: [u8; 20],
    ) -> Vec<u8> {
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let event_name = match kind {
            DeepXDelegateAccountChangeKind::Set => "DelegateAccountSet",
            DeepXDelegateAccountChangeKind::Removed => "DelegateAccountRemoved",
        };
        let event_index = snapshot
            .interfaces()
            .event("Subaccount", event_name)
            .unwrap()
            .index();
        let event = pallet.event_variant_by_index(event_index).unwrap();
        assert_eq!(
            event
                .fields
                .iter()
                .map(|field| field.name.as_deref())
                .collect::<Vec<_>>(),
            [Some("owner"), Some("delegate")],
        );
        let mut bytes = phase.encode();
        bytes.extend([pallet.index(), event.index]);
        for (field, value) in event.fields.iter().zip([
            ScaleValue::from_bytes(owner),
            ScaleValue::from_bytes([0x22; 20]),
        ]) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    #[rstest::rstest]
    #[case::set(DeepXDelegateAccountChangeKind::Set)]
    #[case::removed(DeepXDelegateAccountChangeKind::Removed)]
    fn finalized_delegate_account_change_binds_complete_event_and_dispatch(
        #[case] kind: DeepXDelegateAccountChangeKind,
    ) {
        let snapshot = read_only_runtime_snapshot();
        let location = DeepXFinalizedTransactionLocation {
            extrinsic_hash: [9; 32],
            block_number: 42,
            block_hash: [3; 32],
            extrinsic_index: 7,
        };
        let events = system_events(&[
            dispatch_event_record(&snapshot, 6, true),
            delegate_account_event_record(&snapshot, Phase::ApplyExtrinsic(7), kind),
            dispatch_event_record(&snapshot, 7, true),
        ]);
        let decoded =
            decode_finalized_delegate_account_changes_with_runtime(&snapshot, [3; 32], 42, &events)
                .unwrap();
        let change = verify_finalized_delegate_account_change_events_with_runtime(
            &snapshot, location, &events, [0x11; 20], [0x22; 20], kind,
        )
        .unwrap();
        assert_eq!(decoded, vec![change.clone()]);
        assert_eq!(change.block_hash, [3; 32]);
        assert_eq!(change.block_number, 42);
        assert_eq!(change.event_index, 1);
        assert_eq!(change.extrinsic_index, 7);
        assert_eq!(change.owner, [0x11; 20]);
        assert_eq!(change.delegate, [0x22; 20]);
        assert_eq!(change.kind, kind);
    }

    #[rstest::rstest]
    fn finalized_delegate_account_change_rejects_incomplete_or_conflicting_evidence() {
        let snapshot = read_only_runtime_snapshot();
        let location = DeepXFinalizedTransactionLocation {
            extrinsic_hash: [9; 32],
            block_number: 42,
            block_hash: [3; 32],
            extrinsic_index: 7,
        };
        let set = delegate_account_event_record(
            &snapshot,
            Phase::ApplyExtrinsic(7),
            DeepXDelegateAccountChangeKind::Set,
        );
        let success = dispatch_event_record(&snapshot, 7, true);
        let valid = system_events(&[set.clone(), success.clone()]);
        let mut trailing = valid.clone();
        trailing.push(0);
        let mut wrong_count = valid.clone();
        wrong_count[0] = Compact(3_u32).encode()[0];
        for bytes in [&trailing, &wrong_count, &valid[..valid.len() - 1]] {
            assert!(
                decode_finalized_delegate_account_changes_with_runtime(
                    &snapshot, [3; 32], 42, bytes,
                )
                .is_err()
            );
        }
        let wrong_phase = system_events(&[delegate_account_event_record(
            &snapshot,
            Phase::Initialization,
            DeepXDelegateAccountChangeKind::Set,
        )]);
        assert!(matches!(
            decode_finalized_delegate_account_changes_with_runtime(
                &snapshot,
                [3; 32],
                42,
                &wrong_phase,
            ),
            Err(DeepXDelegateAccountEventError::InvalidChange(0))
        ));
        let verify = |records: &[Vec<u8>], owner, kind| {
            verify_finalized_delegate_account_change_events_with_runtime(
                &snapshot,
                location,
                &system_events(records),
                owner,
                [0x22; 20],
                kind,
            )
        };
        assert!(matches!(
            verify(
                &[set.clone(), success.clone()],
                [0x33; 20],
                DeepXDelegateAccountChangeKind::Set
            ),
            Err(DeepXDelegateAccountVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify_finalized_delegate_account_change_events_with_runtime(
                &snapshot,
                location,
                &valid,
                [0x11; 20],
                [0x33; 20],
                DeepXDelegateAccountChangeKind::Set,
            ),
            Err(DeepXDelegateAccountVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify(
                &[set.clone(), success.clone()],
                [0x11; 20],
                DeepXDelegateAccountChangeKind::Removed
            ),
            Err(DeepXDelegateAccountVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify(
                &[set.clone(), set.clone(), success.clone()],
                [0x11; 20],
                DeepXDelegateAccountChangeKind::Set
            ),
            Err(DeepXDelegateAccountVerificationError::Change)
        ));
        assert!(matches!(
            verify(
                std::slice::from_ref(&set),
                [0x11; 20],
                DeepXDelegateAccountChangeKind::Set
            ),
            Err(DeepXDelegateAccountVerificationError::Dispatch)
        ));
        assert!(matches!(
            verify(
                &[set.clone(), dispatch_event_record(&snapshot, 7, false)],
                [0x11; 20],
                DeepXDelegateAccountChangeKind::Set
            ),
            Err(DeepXDelegateAccountVerificationError::Dispatch)
        ));
        assert!(matches!(
            verify(
                &[set, success.clone(), success],
                [0x11; 20],
                DeepXDelegateAccountChangeKind::Set
            ),
            Err(DeepXDelegateAccountVerificationError::Dispatch)
        ));
    }

    fn delegate_mode_event_record(
        snapshot: &impl RuntimeMetadataView,
        phase: Phase,
        mode: &str,
    ) -> Vec<u8> {
        delegate_mode_event_record_for_owner(snapshot, phase, mode, [0x11; 20])
    }

    fn delegate_mode_event_record_for_owner(
        snapshot: &impl RuntimeMetadataView,
        phase: Phase,
        mode: &str,
        owner: [u8; 20],
    ) -> Vec<u8> {
        let pallet = snapshot.metadata().pallet_by_name("Subaccount").unwrap();
        let event_index = snapshot
            .interfaces()
            .event("Subaccount", "DelegateAccountModeUpdated")
            .unwrap()
            .index();
        let event = pallet.event_variant_by_index(event_index).unwrap();
        assert_eq!(
            event
                .fields
                .iter()
                .map(|field| field.name.as_deref())
                .collect::<Vec<_>>(),
            [Some("owner"), Some("delegate"), Some("new_mode")],
        );
        let mut bytes = phase.encode();
        bytes.extend([pallet.index(), event.index]);
        for (field, value) in event.fields.iter().zip([
            ScaleValue::from_bytes(owner),
            ScaleValue::from_bytes([0x22; 20]),
            ScaleValue::unnamed_variant(mode, []),
        ]) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut bytes,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut bytes);
        bytes
    }

    #[rstest::rstest]
    #[case::trading("PlaceOrCancelOrder", DeepXDelegateMode::PlaceOrCancelOrder)]
    #[case::deposit("DepositOrWithdraw", DeepXDelegateMode::DepositOrWithdraw)]
    #[case::subaccount("UpdateSubaccount", DeepXDelegateMode::UpdateSubaccount)]
    #[case::disabled("Disable", DeepXDelegateMode::Disable)]
    fn finalized_delegate_mode_changes_decode_complete_approved_events(
        #[case] variant: &str,
        #[case] expected_mode: DeepXDelegateMode,
    ) {
        let snapshot = read_only_runtime_snapshot();
        let dispatch = dispatch_event_record(&snapshot, 7, true);
        let change = delegate_mode_event_record(&snapshot, Phase::ApplyExtrinsic(7), variant);
        let events = system_events(&[dispatch.clone(), change]);
        let decoded =
            decode_finalized_delegate_mode_changes_with_runtime(&snapshot, [3; 32], 42, &events)
                .unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].block_hash, [3; 32]);
        assert_eq!(decoded[0].block_number, 42);
        assert_eq!(decoded[0].event_index, 1);
        assert_eq!(decoded[0].extrinsic_index, 7);
        assert_eq!(decoded[0].owner, [0x11; 20]);
        assert_eq!(decoded[0].delegate, [0x22; 20]);
        assert_eq!(decoded[0].mode, expected_mode);
        assert!(
            decode_finalized_delegate_mode_changes_with_runtime(
                &snapshot,
                [3; 32],
                42,
                &system_events(&[dispatch]),
            )
            .unwrap()
            .is_empty()
        );
    }

    #[rstest::rstest]
    fn finalized_delegate_mode_changes_reject_incomplete_or_invalid_events() {
        let snapshot = runtime_snapshot();
        let change =
            delegate_mode_event_record(&snapshot, Phase::ApplyExtrinsic(7), "PlaceOrCancelOrder");
        let complete = system_events(std::slice::from_ref(&change));
        let mut trailing = complete.clone();
        trailing.push(0);
        let mut wrong_count = complete.clone();
        wrong_count[0] = Compact(2_u32).encode()[0];
        let truncated = &complete[..complete.len() - 1];
        for bytes in [&trailing[..], &wrong_count, truncated] {
            assert!(
                decode_finalized_delegate_mode_changes_with_runtime(&snapshot, [3; 32], 42, bytes,)
                    .is_err()
            );
        }
        let wrong_phase = system_events(&[delegate_mode_event_record(
            &snapshot,
            Phase::Initialization,
            "Disable",
        )]);
        assert!(matches!(
            decode_finalized_delegate_mode_changes_with_runtime(
                &snapshot,
                [3; 32],
                42,
                &wrong_phase,
            ),
            Err(DeepXDelegateModeEventError::InvalidChange(0))
        ));
        let mut invalid_mode = change;
        let phase_length = Phase::ApplyExtrinsic(7).encode().len();
        invalid_mode[phase_length + 2 + 40] = u8::MAX;
        assert!(
            decode_finalized_delegate_mode_changes_with_runtime(
                &snapshot,
                [3; 32],
                42,
                &system_events(&[invalid_mode]),
            )
            .is_err()
        );
    }

    #[rstest::rstest]
    fn finalized_delegate_mode_change_binds_successful_dispatch_and_terms() {
        let snapshot = runtime_snapshot();
        let location = DeepXFinalizedTransactionLocation {
            extrinsic_hash: [9; 32],
            block_number: 42,
            block_hash: [3; 32],
            extrinsic_index: 7,
        };
        let events = system_events(&[
            dispatch_event_record(&snapshot, 6, true),
            delegate_mode_event_record(&snapshot, Phase::ApplyExtrinsic(6), "Disable"),
            delegate_mode_event_record(&snapshot, Phase::ApplyExtrinsic(7), "PlaceOrCancelOrder"),
            dispatch_event_record(&snapshot, 7, true),
        ]);
        let change = verify_finalized_delegate_mode_change_events_with_runtime(
            &snapshot,
            location,
            &events,
            [0x11; 20],
            [0x22; 20],
            DeepXDelegateMode::PlaceOrCancelOrder,
        )
        .unwrap();
        assert_eq!(change.block_hash, [3; 32]);
        assert_eq!(change.block_number, 42);
        assert_eq!(change.event_index, 2);
        assert_eq!(change.extrinsic_index, 7);
        assert_eq!(change.owner, [0x11; 20]);
        assert_eq!(change.delegate, [0x22; 20]);
        assert_eq!(change.mode, DeepXDelegateMode::PlaceOrCancelOrder);
    }

    #[rstest::rstest]
    fn finalized_delegate_mode_change_rejects_conflicting_or_incomplete_evidence() {
        let snapshot = runtime_snapshot();
        let location = DeepXFinalizedTransactionLocation {
            extrinsic_hash: [9; 32],
            block_number: 42,
            block_hash: [3; 32],
            extrinsic_index: 7,
        };
        let change =
            delegate_mode_event_record(&snapshot, Phase::ApplyExtrinsic(7), "PlaceOrCancelOrder");
        let success = dispatch_event_record(&snapshot, 7, true);
        let verify = |records: &[Vec<u8>], owner, mode| {
            verify_finalized_delegate_mode_change_events_with_runtime(
                &snapshot,
                location,
                &system_events(records),
                owner,
                [0x22; 20],
                mode,
            )
        };
        assert!(matches!(
            verify(
                &[change.clone(), success.clone()],
                [0x33; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify_finalized_delegate_mode_change_events_with_runtime(
                &snapshot,
                location,
                &system_events(&[change.clone(), success.clone()]),
                [0x11; 20],
                [0x33; 20],
                DeepXDelegateMode::PlaceOrCancelOrder,
            ),
            Err(DeepXDelegateModeVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify(
                &[change.clone(), success.clone()],
                [0x11; 20],
                DeepXDelegateMode::Disable
            ),
            Err(DeepXDelegateModeVerificationError::ConflictingChange)
        ));
        assert!(matches!(
            verify(
                &[change.clone(), change.clone(), success.clone()],
                [0x11; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::Change)
        ));
        assert!(matches!(
            verify(
                std::slice::from_ref(&change),
                [0x11; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::Dispatch)
        ));
        assert!(matches!(
            verify(
                &[change.clone(), dispatch_event_record(&snapshot, 7, false)],
                [0x11; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::Dispatch)
        ));
        assert!(matches!(
            verify(
                &[change, success.clone(), success.clone()],
                [0x11; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::Dispatch)
        ));
        assert!(matches!(
            verify(
                &[success],
                [0x11; 20],
                DeepXDelegateMode::PlaceOrCancelOrder
            ),
            Err(DeepXDelegateModeVerificationError::Change)
        ));
    }

    #[rstest::rstest]
    fn spot_cancel_approved_metadata_regression() {
        let snapshot = runtime_snapshot();
        let mut wire = serde_json::to_value(cancel_identity([0x11; 20], 7, false)).unwrap();
        wire["operation"] = json!({"type": "spot-cancel", "subaccount": vec![0x11; 20],
            "pair": vec![0x22; 32], "order_id": 7, "is_buy": true, "fast_cancel": false});
        let identity: DeepXTransactionIdentity = serde_json::from_value(wire.clone()).unwrap();
        let pallet = snapshot.metadata().pallet_by_name("SpotMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == "OrderCancelled")
            .unwrap();
        assert_eq!(
            event
                .fields
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["pair", "order_id", "maker", "is_buy", "reason"]
        );
        let values = [
            ScaleValue::from_bytes([0x22; 32]),
            ScaleValue::u128(7),
            ScaleValue::from_bytes([0x11; 20]),
            ScaleValue::bool(true),
            ScaleValue::unnamed_variant("UserCanceled", []),
        ];
        let mut record = Phase::ApplyExtrinsic(3).encode();
        record.extend([pallet.index(), event.index]);
        for (field, value) in event.fields.iter().zip(values) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        let events = system_events(&[dispatch_event_record(&snapshot, 3, true), record.clone()]);
        assert_eq!(
            verify_spot_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [1; 32], 42, 3, &events,
            )
            .unwrap()
            .outcome(),
            DeepXInclusionOutcome::Success
        );
        for name in ["subaccount", "pair", "order_id", "is_buy", "fast_cancel"] {
            let mut changed = wire.clone();
            match name {
                "subaccount" | "pair" => changed["operation"][name][0] = json!(0),
                "order_id" => changed["operation"][name] = json!(8),
                "is_buy" => changed["operation"][name] = json!(false),
                "fast_cancel" => changed["operation"][name] = json!(true),
                _ => unreachable!(),
            }
            let changed = serde_json::from_value(changed).unwrap();
            assert!(
                verify_spot_cancel_inclusion_events_with_runtime(
                    &snapshot, &changed, [1; 32], 42, 3, &events,
                )
                .is_err(),
                "{name}"
            );
        }
        let failed = system_events(&[dispatch_event_record(&snapshot, 3, false)]);
        assert_eq!(
            verify_spot_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [1; 32], 42, 3, &failed,
            )
            .unwrap()
            .outcome(),
            DeepXInclusionOutcome::Failed
        );
        for invalid in [
            system_events(&[dispatch_event_record(&snapshot, 3, true)]),
            system_events(&[dispatch_event_record(&snapshot, 4, true), record.clone()]),
            system_events(&[
                dispatch_event_record(&snapshot, 3, true),
                dispatch_event_record(&snapshot, 3, false),
                record.clone(),
            ]),
            system_events(&[
                dispatch_event_record(&snapshot, 3, true),
                record.clone(),
                record.clone(),
            ]),
            system_events(&[dispatch_event_record(&snapshot, 4, false)]),
            Vec::new(),
            events[..events.len() - 1].to_vec(),
            [events.as_slice(), &[0]].concat(),
        ] {
            assert!(
                verify_spot_cancel_inclusion_events_with_runtime(
                    &snapshot, &identity, [1; 32], 42, 3, &invalid,
                )
                .is_err()
            );
        }
        let mut wrong_index = record.clone();
        wrong_index[1..5].copy_from_slice(&4_u32.to_le_bytes());
        assert!(
            verify_spot_cancel_inclusion_events_with_runtime(
                &snapshot,
                &identity,
                [1; 32],
                42,
                3,
                &system_events(&[dispatch_event_record(&snapshot, 3, true), wrong_index])
            )
            .is_err()
        );
        let mut wrong_reason = record;
        let reason_offset = wrong_reason.len() - 2;
        wrong_reason[reason_offset] = 255;
        assert!(
            verify_spot_cancel_inclusion_events_with_runtime(
                &snapshot,
                &identity,
                [1; 32],
                42,
                3,
                &system_events(&[dispatch_event_record(&snapshot, 3, true), wrong_reason])
            )
            .is_err()
        );
    }

    #[rstest::rstest]
    #[case(true)]
    #[case(false)]
    fn spot_place_approved_metadata_regression(#[case] is_buy: bool) {
        let snapshot = runtime_snapshot();
        let identity = spot_place_identity(is_buy);
        let record = spot_place_event_record(&snapshot, 3, &identity);
        let events = system_events(&[dispatch_event_record(&snapshot, 3, true), record.clone()]);
        assert_eq!(
            verify_spot_place_inclusion_events_with_runtime(
                &snapshot, &identity, [1; 32], 42, 3, &events,
            )
            .unwrap()
            .outcome(),
            DeepXInclusionOutcome::Success
        );

        let mut changed = serde_json::to_value(&identity).unwrap();
        changed["operation"]["quote_amount"][0] = json!(255);
        let changed = serde_json::from_value(changed).unwrap();
        assert!(
            verify_spot_place_inclusion_events_with_runtime(
                &snapshot, &changed, [1; 32], 42, 3, &events,
            )
            .is_err()
        );

        for name in ["runtime", "nonce", "side"] {
            let mut changed = serde_json::to_value(&identity).unwrap();
            match name {
                "runtime" => changed["runtime"]["spec_version"] = json!(367),
                "nonce" => changed["nonce"]["value"] = json!(1_725_000_000_126_u64),
                "side" => {
                    changed["order_side"] = serde_json::to_value(if is_buy {
                        OrderSide::Sell
                    } else {
                        OrderSide::Buy
                    })
                    .unwrap();
                }
                _ => unreachable!(),
            }
            let changed = serde_json::from_value(changed).unwrap();
            assert!(
                verify_spot_place_inclusion_events_with_runtime(
                    &snapshot, &changed, [1; 32], 42, 3, &events,
                )
                .is_err(),
                "{name}"
            );
        }

        let failed = system_events(&[dispatch_event_record(&snapshot, 3, false)]);
        assert_eq!(
            verify_spot_place_inclusion_events_with_runtime(
                &snapshot, &identity, [1; 32], 42, 3, &failed,
            )
            .unwrap()
            .outcome(),
            DeepXInclusionOutcome::Failed
        );
        for invalid in [
            system_events(&[dispatch_event_record(&snapshot, 3, true)]),
            system_events(&[
                dispatch_event_record(&snapshot, 3, true),
                dispatch_event_record(&snapshot, 3, false),
                record.clone(),
            ]),
            system_events(&[
                dispatch_event_record(&snapshot, 3, true),
                spot_place_event_record(&snapshot, 4, &identity),
            ]),
            system_events(&[dispatch_event_record(&snapshot, 4, false)]),
            Vec::new(),
            system_events(&[
                dispatch_event_record(&snapshot, 3, true),
                record.clone(),
                record.clone(),
            ]),
            system_events(&[dispatch_event_record(&snapshot, 4, true), record]),
            events[..events.len() - 1].to_vec(),
            [events.as_slice(), &[0]].concat(),
        ] {
            assert!(
                verify_spot_place_inclusion_events_with_runtime(
                    &snapshot, &identity, [1; 32], 42, 3, &invalid,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn spot_place_policy_metadata_matrix() {
        use crate::signing::{DeepXPostOnlyParam, DeepXSpotOrderType, DeepXTimeInForce};

        let snapshot = runtime_snapshot();
        for is_buy in [true, false] {
            for order_type in [
                DeepXSpotOrderType::Limit(DeepXTimeInForce::Gtc),
                DeepXSpotOrderType::Limit(DeepXTimeInForce::Ioc),
                DeepXSpotOrderType::Limit(DeepXTimeInForce::Fok),
                DeepXSpotOrderType::Market(None),
                DeepXSpotOrderType::Market(Some(u64::MAX)),
                DeepXSpotOrderType::Stop,
            ] {
                for post_only in [
                    DeepXPostOnlyParam::None,
                    DeepXPostOnlyParam::MustPostOnly,
                    DeepXPostOnlyParam::Adaptive,
                ] {
                    let mut wire = serde_json::to_value(spot_place_identity(is_buy)).unwrap();
                    wire["operation"]["order_type"] = serde_json::to_value(order_type).unwrap();
                    wire["operation"]["post_only"] = serde_json::to_value(post_only).unwrap();
                    wire["operation"]["reduce_only"] = json!(true);
                    let identity = serde_json::from_value(wire.clone()).unwrap();
                    let events = system_events(&[
                        dispatch_event_record(&snapshot, 3, true),
                        spot_place_event_record(&snapshot, 3, &identity),
                    ]);
                    assert_eq!(
                        verify_spot_place_inclusion_events_with_runtime(
                            &snapshot, &identity, [1; 32], 42, 3, &events
                        )
                        .unwrap()
                        .outcome(),
                        DeepXInclusionOutcome::Success
                    );
                    for name in [
                        "subaccount",
                        "pair",
                        "quote_amount",
                        "base_amount",
                        "order_type",
                        "post_only",
                        "reduce_only",
                        "is_buy",
                    ] {
                        let mut changed = wire.clone();
                        match name {
                            "subaccount" | "pair" | "quote_amount" | "base_amount" => {
                                changed["operation"][name][0] = json!(255);
                            }
                            "order_type" => {
                                let other = if order_type == DeepXSpotOrderType::Stop {
                                    DeepXSpotOrderType::Market(None)
                                } else {
                                    DeepXSpotOrderType::Stop
                                };
                                changed["operation"][name] = serde_json::to_value(other).unwrap();
                            }
                            "post_only" => {
                                let other = if post_only == DeepXPostOnlyParam::None {
                                    DeepXPostOnlyParam::Adaptive
                                } else {
                                    DeepXPostOnlyParam::None
                                };
                                changed["operation"][name] = serde_json::to_value(other).unwrap();
                            }
                            "reduce_only" => changed["operation"][name] = json!(false),
                            "is_buy" => changed["operation"][name] = json!(!is_buy),
                            _ => unreachable!(),
                        }
                        let changed = serde_json::from_value(changed).unwrap();
                        assert!(
                            verify_spot_place_inclusion_events_with_runtime(
                                &snapshot, &changed, [1; 32], 42, 3, &events
                            )
                            .is_err(),
                            "{order_type:?} {post_only:?} {name}"
                        );
                    }
                }
            }
        }
    }

    #[rstest::rstest]
    fn perpetual_cancel_event_requires_exact_index_and_fields() {
        let snapshot = runtime_snapshot();
        let subaccount = [42; 20];
        let identity = cancel_identity(subaccount, 9001, false);
        let events = system_events(&[cancel_event_record(&snapshot, 3, subaccount, 9001, 0)]);

        assert_eq!(
            verify_perp_cancel_business_event_with_runtime(&snapshot, &identity, 3, &events)
                .unwrap(),
            DeepXIndexedOutcome {
                extrinsic_index: 3,
                outcome: DeepXBusinessEventOutcome::Success,
            },
        );
    }

    #[rstest::rstest]
    fn perpetual_cancel_inclusion_requires_same_index_dispatch_and_business_event() {
        let snapshot = runtime_snapshot();
        let subaccount = [42; 20];
        let identity = cancel_identity(subaccount, 9001, false);
        let events = system_events(&[
            cancel_event_record(&snapshot, 3, subaccount, 9001, 0),
            dispatch_event_record(&snapshot, 3, true),
        ]);

        let inclusion = verify_perp_cancel_inclusion_events_with_runtime(
            &snapshot, &identity, [41; 32], 41, 3, &events,
        )
        .unwrap();

        assert_eq!(inclusion.block_hash(), [41; 32]);
        assert_eq!(inclusion.block_number(), 41);
        assert_eq!(inclusion.extrinsic_index(), 3);
        assert_eq!(
            inclusion.outcome(),
            super::super::DeepXInclusionOutcome::Success
        );
    }

    #[rstest::rstest]
    fn perpetual_cancel_inclusion_rejects_missing_business_event() {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, false);
        let events = system_events(&[dispatch_event_record(&snapshot, 3, true)]);

        assert!(matches!(
            verify_perp_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [41; 32], 41, 3, &events,
            ),
            Err(DeepXPerpCancelEventVerificationError::InclusionEvidence(_)),
        ));
    }

    #[rstest::rstest]
    #[case::success(true, DeepXInclusionOutcome::Success)]
    #[case::failed(false, DeepXInclusionOutcome::Failed)]
    fn fast_perpetual_cancel_inclusion_uses_same_index_dispatch(
        #[case] success: bool,
        #[case] expected: DeepXInclusionOutcome,
    ) {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, true);
        let events = system_events(&[
            cancel_event_record(&snapshot, 4, [42; 20], 9001, 0),
            dispatch_event_record(&snapshot, 3, success),
        ]);

        let inclusion = verify_perp_cancel_inclusion_events_with_runtime(
            &snapshot, &identity, [41; 32], 41, 3, &events,
        )
        .unwrap();

        assert_eq!(inclusion.extrinsic_index(), 3);
        assert_eq!(inclusion.outcome(), expected);
    }

    #[rstest::rstest]
    fn fast_perpetual_cancel_inclusion_rejects_duplicate_dispatch() {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, true);
        let events = system_events(&[
            dispatch_event_record(&snapshot, 3, true),
            dispatch_event_record(&snapshot, 3, true),
        ]);

        assert!(matches!(
            verify_perp_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [41; 32], 41, 3, &events,
            ),
            Err(DeepXPerpCancelEventVerificationError::DuplicateEvent),
        ));
    }

    #[rstest::rstest]
    fn fast_perpetual_cancel_inclusion_rejects_wrong_dispatch_index() {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, true);
        let events = system_events(&[dispatch_event_record(&snapshot, 4, true)]);

        assert!(matches!(
            verify_perp_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [41; 32], 41, 3, &events,
            ),
            Err(DeepXPerpCancelEventVerificationError::MalformedEventBytes),
        ));
    }

    #[rstest::rstest]
    fn fast_perpetual_cancel_inclusion_rejects_trailing_event_bytes() {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, true);
        let mut events = system_events(&[dispatch_event_record(&snapshot, 3, true)]);
        events.push(0);

        assert!(matches!(
            verify_perp_cancel_inclusion_events_with_runtime(
                &snapshot, &identity, [41; 32], 41, 3, &events,
            ),
            Err(DeepXPerpCancelEventVerificationError::MalformedEventBytes),
        ));
    }

    #[rstest::rstest]
    fn absent_or_other_extrinsic_cancel_event_is_not_observed() {
        let snapshot = runtime_snapshot();
        let subaccount = [42; 20];
        let identity = cancel_identity(subaccount, 9001, false);

        for events in [
            system_events(&[]),
            system_events(&[cancel_event_record(&snapshot, 4, subaccount, 9001, 0)]),
            system_events(&[cancel_event_record_with_phase(
                &snapshot,
                Phase::Finalization,
                subaccount,
                9001,
                0,
            )]),
        ] {
            assert_eq!(
                verify_perp_cancel_business_event_with_runtime(&snapshot, &identity, 3, &events)
                    .unwrap(),
                DeepXIndexedOutcome {
                    extrinsic_index: 3,
                    outcome: DeepXBusinessEventOutcome::NotObserved,
                },
            );
        }
    }

    #[rstest::rstest]
    #[case::wrong_user([41; 20], 9001, 0)]
    #[case::wrong_order([42; 20], 9002, 0)]
    #[case::wrong_reason([42; 20], 9001, 1)]
    fn conflicting_perpetual_cancel_event_is_rejected(
        #[case] subaccount: [u8; 20],
        #[case] order_id: u64,
        #[case] reason_index: u8,
    ) {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, false);
        let events = system_events(&[cancel_event_record(
            &snapshot,
            3,
            subaccount,
            order_id,
            reason_index,
        )]);

        assert!(matches!(
            verify_perp_cancel_business_event_with_runtime(&snapshot, &identity, 3, &events),
            Err(DeepXPerpCancelEventVerificationError::ConflictingEvent),
        ));
    }

    #[rstest::rstest]
    fn duplicate_perpetual_cancel_event_is_rejected() {
        let snapshot = runtime_snapshot();
        let subaccount = [42; 20];
        let identity = cancel_identity(subaccount, 9001, false);
        let record = cancel_event_record(&snapshot, 3, subaccount, 9001, 0);
        let events = system_events(&[record.clone(), record]);

        assert!(matches!(
            verify_perp_cancel_business_event_with_runtime(&snapshot, &identity, 3, &events),
            Err(DeepXPerpCancelEventVerificationError::DuplicateEvent),
        ));
    }

    #[rstest::rstest]
    fn malformed_perpetual_cancel_event_bytes_are_rejected() {
        let snapshot = runtime_snapshot();
        let identity = cancel_identity([42; 20], 9001, false);
        let mut trailing = system_events(&[]);
        trailing.push(0);

        for events in [vec![4], trailing] {
            assert!(matches!(
                verify_perp_cancel_business_event_with_runtime(&snapshot, &identity, 3, &events),
                Err(DeepXPerpCancelEventVerificationError::MalformedEventBytes),
            ));
        }
    }

    #[rstest::rstest]
    fn unsupported_and_fast_cancel_operations_are_rejected() {
        let snapshot = runtime_snapshot();
        let fast_cancel = cancel_identity([42; 20], 9001, true);
        let key = DeepXPrivateKey::new(
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &DeepXKeyScheme::Secp256k1,
        )
        .unwrap();
        let legacy = DeepXTransactionIdentity::new(
            ClientOrderId::new("O-19700101-000000-001-001-1"),
            derive_signer_account_id(&key).unwrap(),
            InstrumentId::from_as_ref("ETH-USDC-PERP.DEEPX").unwrap(),
            OrderSide::Buy,
            DeepXNonceReservation::TimestampOrderId {
                value: 1_725_000_000_125,
            },
            DeepXDirectRuntimeIdentity::from(snapshot.identity()),
        );

        assert!(matches!(
            verify_perp_cancel_business_event_with_runtime(&snapshot, &fast_cancel, 3, &[0]),
            Err(DeepXPerpCancelEventVerificationError::FastCancelUnsupported),
        ));
        assert!(matches!(
            verify_perp_cancel_business_event_with_runtime(&snapshot, &legacy, 3, &[0]),
            Err(DeepXPerpCancelEventVerificationError::UnsupportedOperation),
        ));
    }

    #[rstest::rstest]
    #[case::canonical([42; 32], Some(3), DeepXReorganizationDecision::Canonical)]
    #[case::reorganized(
        [43; 32],
        None,
        DeepXReorganizationDecision::Reorganized(inclusion([42; 32], 42, 3)),
    )]
    #[case::missing([42; 32], None, DeepXReorganizationDecision::ActionRequired)]
    #[case::displaced([42; 32], Some(4), DeepXReorganizationDecision::ActionRequired)]
    fn canonical_observation_classifies_reorganization_fail_closed(
        #[case] block_hash: [u8; 32],
        #[case] extrinsic_index: Option<u32>,
        #[case] expected: DeepXReorganizationDecision,
    ) {
        let recorded = inclusion([42; 32], 42, 3);
        let observation = DeepXCanonicalBlockObservation {
            block_number: 42,
            block_hash,
            extrinsic_index,
        };

        assert_eq!(
            classify_canonical_block_observation(recorded, observation),
            expected,
        );
    }

    #[derive(Clone)]
    struct RecoveryRpcState {
        finalized_block: u64,
        finalized_hash: Option<String>,
        block_extrinsics: Arc<BTreeMap<u64, Vec<String>>>,
        unavailable_blocks: Arc<HashSet<u64>>,
        pool_extrinsics: Arc<[String]>,
        event_storage: Arc<BTreeMap<(u64, String), String>>,
        post_checkpoint_requests: Arc<AtomicUsize>,
    }

    async fn recovery_rpc(
        State(state): State<RecoveryRpcState>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
        let method = request["method"].as_str().unwrap();
        if matches!(
            method,
            "chain_getFinalizedHead"
                | "chain_getHeader"
                | "chain_getBlock"
                | "state_getStorage"
                | "author_pendingExtrinsics"
        ) {
            state
                .post_checkpoint_requests
                .fetch_add(1, Ordering::Relaxed);
        }
        let result = match method {
            "rpc_methods" => json!({
                "methods": [
                    "author_pendingExtrinsics",
                    "author_submitExtrinsic",
                    "chain_getBlock",
                    "chain_getBlockHash",
                    "chain_getFinalizedHead",
                    "chain_getHeader",
                    "eth_call",
                    "state_getStorage",
                    "state_queryStorageAt",
                    "state_call",
                ],
            }),
            "chain_getFinalizedHead" => json!(
                state
                    .finalized_hash
                    .clone()
                    .unwrap_or_else(|| block_hash(state.finalized_block))
            ),
            "chain_getHeader" => json!({ "number": format!("0x{:x}", state.finalized_block) }),
            "chain_getBlockHash" => {
                let number = request["params"][0].as_u64().unwrap();
                json!(block_hash(number))
            }
            "chain_getBlock" => {
                let number = decode_mock_block_hash(request["params"][0].as_str().unwrap());
                if state.unavailable_blocks.contains(&number) {
                    Value::Null
                } else {
                    json!({
                        "block": {
                            "header": { "number": format!("0x{number:x}") },
                            "extrinsics": state
                                .block_extrinsics
                                .get(&number)
                                .map(Vec::as_slice)
                                .unwrap_or_default(),
                        },
                    })
                }
            }
            "state_getStorage" => {
                let key = request["params"][0].as_str().unwrap();
                let number = decode_mock_block_hash(request["params"][1].as_str().unwrap());
                json!(state.event_storage.get(&(number, key.to_string())))
            }
            "author_pendingExtrinsics" => json!(state.pool_extrinsics.as_ref()),
            method => panic!("unexpected method {method}"),
        };
        Json(json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
    }

    fn block_hash(number: u64) -> String {
        format!("0x{number:064x}")
    }

    fn decode_mock_block_hash(encoded: &str) -> u64 {
        u64::from_str_radix(encoded.trim_start_matches("0x"), 16).unwrap()
    }

    async fn recovery_endpoints(
        finalized_block: u64,
        finalized_hash: Option<String>,
        block_extrinsics: BTreeMap<u64, Vec<String>>,
        pool_extrinsics: Vec<String>,
    ) -> (
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
        Arc<AtomicUsize>,
    ) {
        recovery_endpoints_with_events(
            finalized_block,
            finalized_hash,
            block_extrinsics,
            pool_extrinsics,
            BTreeMap::new(),
        )
        .await
    }

    async fn recovery_endpoints_with_events(
        finalized_block: u64,
        finalized_hash: Option<String>,
        block_extrinsics: BTreeMap<u64, Vec<String>>,
        pool_extrinsics: Vec<String>,
        event_storage: BTreeMap<u64, String>,
    ) -> (
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
        Arc<AtomicUsize>,
    ) {
        let event_storage = event_storage
            .into_iter()
            .map(|(block_number, value)| {
                ((block_number, SYSTEM_EVENTS_STORAGE_KEY.to_string()), value)
            })
            .collect();
        recovery_endpoints_with_storage(
            finalized_block,
            finalized_hash,
            block_extrinsics,
            pool_extrinsics,
            event_storage,
        )
        .await
    }

    async fn recovery_endpoints_with_storage(
        finalized_block: u64,
        finalized_hash: Option<String>,
        block_extrinsics: BTreeMap<u64, Vec<String>>,
        pool_extrinsics: Vec<String>,
        event_storage: BTreeMap<(u64, String), String>,
    ) -> (
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let post_checkpoint_requests = Arc::new(AtomicUsize::new(0));
        let state = RecoveryRpcState {
            finalized_block,
            finalized_hash,
            block_extrinsics: Arc::new(block_extrinsics),
            unavailable_blocks: Arc::new(HashSet::new()),
            pool_extrinsics: Arc::from(pool_extrinsics),
            event_storage: Arc::new(event_storage),
            post_checkpoint_requests: Arc::clone(&post_checkpoint_requests),
        };
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(recovery_rpc))
                    .with_state(state),
            )
            .await
            .unwrap();
        });
        let url = format!("http://{address}");
        let config = DeepXNetworkConfig {
            base_url_rpc: Some(url.clone()),
            ..Default::default()
        };
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let endpoints = validate_rpc_endpoint_identities(
            &config,
            [
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Submission, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Recovery, url, genesis_hash),
            ],
        )
        .unwrap();
        let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap();
        (endpoints, capabilities, post_checkpoint_requests)
    }

    async fn unavailable_block_endpoints(block_number: u64) -> DeepXValidatedRpcEndpoints {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = RecoveryRpcState {
            finalized_block: block_number,
            finalized_hash: None,
            block_extrinsics: Arc::new(BTreeMap::new()),
            unavailable_blocks: Arc::new(HashSet::from([block_number])),
            pool_extrinsics: Arc::from([]),
            event_storage: Arc::new(BTreeMap::new()),
            post_checkpoint_requests: Arc::new(AtomicUsize::new(0)),
        };
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(recovery_rpc))
                    .with_state(state),
            )
            .await
            .unwrap();
        });
        let url = format!("http://{address}");
        let config = DeepXNetworkConfig {
            base_url_rpc: Some(url.clone()),
            ..Default::default()
        };
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        validate_rpc_endpoint_identities(
            &config,
            [
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Submission, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Watch, url.clone(), genesis_hash),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Recovery, url, genesis_hash),
            ],
        )
        .unwrap()
    }

    #[rstest]
    fn finalized_event_storage_uses_current_metadata_layout() {
        let snapshot = runtime_snapshot();
        let system = snapshot.metadata().pallet_by_name("System").unwrap();
        let storage = system.storage().unwrap();
        assert_eq!(storage.prefix(), "System");
        let events = storage.entry_by_name("Events").unwrap();
        assert_eq!(events.entry_type().key_ty(), None);
        assert!(storage.entry_by_name("Threads").is_none());
        assert!(storage.entry_by_name("EventsMap").is_none());
    }

    #[rstest]
    fn finalized_perp_trade_decoder_accepts_first_generated_order_id() {
        let snapshot = runtime_snapshot();
        let pallet = snapshot.metadata().pallet_by_name("PerpMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|variant| variant.name == "NewTrade")
            .unwrap();
        let trade = ScaleValue::named_composite([
            ("market_id", ScaleValue::u128(3)),
            ("buyer_order_id", ScaleValue::u128(0)),
            ("buyer", ScaleValue::from_bytes([0x11; 20])),
            ("seller_order_id", ScaleValue::u128(7)),
            ("seller", ScaleValue::from_bytes([0x22; 20])),
            ("price", ScaleValue::u128(2_000_000_000)),
            ("size", ScaleValue::u128(1_000_000_000_000_000_000)),
            ("buyer_leverage", ScaleValue::u128(1_000)),
            ("seller_leverage", ScaleValue::u128(1_000)),
            ("created_at", ScaleValue::u128(42)),
            ("filled_direction", ScaleValue::unnamed_variant("Long", [])),
        ]);
        let fields = ScaleValue::named_composite([
            ("trade", trade),
            ("taker", ScaleValue::unnamed_variant("Buyer", [])),
            ("taker_fee", ScaleValue::i128(1_000)),
            ("taker_fee_asset", ScaleValue::from_bytes(b"USDC")),
            ("maker_fee", ScaleValue::i128(0)),
            ("maker_fee_asset", ScaleValue::from_bytes(b"USDC")),
        ]);
        let ValueDef::Composite(Composite::Named(values)) = &fields.value else {
            panic!("expected named NewTrade fields");
        };
        let mut record = Phase::ApplyExtrinsic(3).encode();
        record.extend([pallet.index(), event.index]);
        for field in &event.fields {
            let value = &values
                .iter()
                .find(|(name, _)| Some(name) == field.name.as_ref())
                .unwrap()
                .1;
            subxt_core::ext::scale_value::scale::encode_as_type(
                value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);

        let trades = decode_finalized_perp_trade_events_with_metadata(
            snapshot.interfaces(),
            snapshot.metadata(),
            [3; 32],
            42,
            &system_events(&[record]),
        )
        .unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].buyer_order_id, 0);
        assert_eq!(trades[0].seller_order_id, 7);
    }

    #[tokio::test]
    async fn finalized_event_storage_rejects_missing_and_malformed_values() {
        for encoded in [None, Some("00"), Some("0x"), Some("0xgg"), Some("0x0")] {
            let storage = encoded
                .map(|value| BTreeMap::from([(41, value.to_string())]))
                .unwrap_or_default();
            let (endpoints, _, _) =
                recovery_endpoints_with_events(42, None, BTreeMap::new(), vec![], storage).await;
            let hash = hex::decode_array(block_hash(41).trim_start_matches("0x")).unwrap();
            let result =
                fetch_system_events(endpoints.url_for(DeepXRpcRole::Recovery), hash, 41).await;
            match encoded {
                None => assert!(matches!(
                    result,
                    Err(DeepXTransactionWatchError::EventStorageUnavailable(41))
                )),
                Some(_) => assert!(matches!(
                    result,
                    Err(DeepXTransactionWatchError::InvalidEventStorage(41))
                )),
            }
        }
    }

    #[tokio::test]
    async fn finalized_event_storage_preserves_exact_bytes_at_requested_block() {
        let storage = BTreeMap::from([(41, "0x00aBff".to_string()), (42, "0x04".to_string())]);
        let (endpoints, _, _) =
            recovery_endpoints_with_events(42, None, BTreeMap::new(), vec![], storage).await;
        let hash = hex::decode_array(block_hash(41).trim_start_matches("0x")).unwrap();
        let bytes = fetch_system_events(endpoints.url_for(DeepXRpcRole::Recovery), hash, 41)
            .await
            .unwrap();
        assert_eq!(bytes, vec![0, 171, 255]);
    }

    #[tokio::test]
    async fn finalized_event_storage_combines_every_thread_through_inclusive_last_index() {
        let block_number = 41;
        let mut first = Compact(1u32).encode();
        first.push(0xaa);
        let mut second = Compact(2u32).encode();
        second.extend_from_slice(&[0xbb, 0xcc]);
        let third = Compact(0u32).encode();
        let storage = BTreeMap::from([
            (
                (block_number, system_threads_storage_key(block_number)),
                "0x02".to_string(),
            ),
            (
                (block_number, system_events_map_storage_key(block_number, 0)),
                format!("0x{}", hex::encode(&first)),
            ),
            (
                (block_number, system_events_map_storage_key(block_number, 1)),
                format!("0x{}", hex::encode(&second)),
            ),
            (
                (block_number, system_events_map_storage_key(block_number, 2)),
                format!("0x{}", hex::encode(&third)),
            ),
        ]);
        let (endpoints, _, _) =
            recovery_endpoints_with_storage(42, None, BTreeMap::new(), vec![], storage).await;
        let hash = hex::decode_array(block_hash(block_number).trim_start_matches("0x")).unwrap();

        let bytes = fetch_system_events(
            endpoints.url_for(DeepXRpcRole::Recovery),
            hash,
            block_number,
        )
        .await
        .unwrap();

        let mut expected = Compact(3u32).encode();
        expected.extend_from_slice(&[0xaa, 0xbb, 0xcc]);
        assert_eq!(bytes, expected);
    }

    #[tokio::test]
    async fn finalized_event_storage_rejects_missing_thread_batch() {
        let block_number = 41;
        let storage = BTreeMap::from([
            (
                (block_number, system_threads_storage_key(block_number)),
                "0x01".to_string(),
            ),
            (
                (block_number, system_events_map_storage_key(block_number, 0)),
                "0x00".to_string(),
            ),
        ]);
        let (endpoints, _, _) =
            recovery_endpoints_with_storage(42, None, BTreeMap::new(), vec![], storage).await;
        let hash = hex::decode_array(block_hash(block_number).trim_start_matches("0x")).unwrap();

        let result = fetch_system_events(
            endpoints.url_for(DeepXRpcRole::Recovery),
            hash,
            block_number,
        )
        .await;

        assert!(matches!(
            result,
            Err(DeepXTransactionWatchError::EventStorageUnavailable(41)),
        ));
    }

    #[tokio::test]
    async fn finalized_event_storage_rejects_malformed_last_thread() {
        let block_number = 41;
        for encoded in ["0x", "0x0001", "0xgg"] {
            let storage = BTreeMap::from([(
                (block_number, system_threads_storage_key(block_number)),
                encoded.to_string(),
            )]);
            let (endpoints, _, _) =
                recovery_endpoints_with_storage(42, None, BTreeMap::new(), vec![], storage).await;
            let hash =
                hex::decode_array(block_hash(block_number).trim_start_matches("0x")).unwrap();

            let result = fetch_system_events(
                endpoints.url_for(DeepXRpcRole::Recovery),
                hash,
                block_number,
            )
            .await;

            assert!(matches!(
                result,
                Err(DeepXTransactionWatchError::InvalidEventStorage(41)),
            ));
        }
    }

    async fn spawn_recovery_rpc(
        finalized_block: u64,
        block_extrinsics: BTreeMap<u64, Vec<String>>,
        pool_extrinsics: Vec<String>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = RecoveryRpcState {
            finalized_block,
            finalized_hash: None,
            block_extrinsics: Arc::new(block_extrinsics),
            unavailable_blocks: Arc::new(HashSet::new()),
            pool_extrinsics: Arc::from(pool_extrinsics),
            event_storage: Arc::new(BTreeMap::new()),
            post_checkpoint_requests: Arc::new(AtomicUsize::new(0)),
        };
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(recovery_rpc))
                    .with_state(state),
            )
            .await
            .unwrap();
        });
        format!("http://{address}")
    }

    async fn split_recovery_endpoints(
        submission_pool: Vec<String>,
        recovery_pool: Vec<String>,
    ) -> (
        DeepXValidatedRpcEndpoints,
        DeepXValidatedRpcMethodCapabilities,
    ) {
        let submission_url = spawn_recovery_rpc(42, BTreeMap::new(), submission_pool).await;
        let recovery_url = spawn_recovery_rpc(42, BTreeMap::new(), recovery_pool).await;
        let config = DeepXNetworkConfig {
            base_url_rpc_submission: Some(submission_url.clone()),
            base_url_rpc_watch: Some(recovery_url.clone()),
            base_url_rpc_recovery: Some(recovery_url.clone()),
            ..Default::default()
        };
        let genesis_hash =
            hex::decode_array(DEEPX_TESTNET_GENESIS_HASH.trim_start_matches("0x")).unwrap();
        let endpoints = validate_rpc_endpoint_identities(
            &config,
            [
                DeepXObservedRpcEndpoint::new(
                    DeepXRpcRole::Submission,
                    submission_url,
                    genesis_hash,
                ),
                DeepXObservedRpcEndpoint::new(
                    DeepXRpcRole::Watch,
                    recovery_url.clone(),
                    genesis_hash,
                ),
                DeepXObservedRpcEndpoint::new(DeepXRpcRole::Recovery, recovery_url, genesis_hash),
            ],
        )
        .unwrap();
        let capabilities = observe_and_validate_rpc_method_capabilities(&endpoints)
            .await
            .unwrap();
        (endpoints, capabilities)
    }

    #[tokio::test]
    async fn canonical_block_locates_exact_extrinsic_hash() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let endpoints = endpoints(
            vec![
                format!("0x{}", hex::encode(extrinsic(&[5, 6]))),
                format!("0x{}", hex::encode(target)),
            ],
            "0x2a",
        )
        .await;

        let observation = observe_canonical_block(&endpoints, 42, target_hash)
            .await
            .unwrap();

        assert_eq!(observation.block_number(), 42);
        assert_eq!(observation.block_hash(), [42; 32]);
        assert_eq!(observation.extrinsic_index(), Some(1));
    }

    #[tokio::test]
    async fn canonical_block_does_not_infer_inclusion_for_absent_hash() {
        let endpoints = endpoints(
            vec![format!("0x{}", hex::encode(extrinsic(&[1, 2, 3])))],
            "0x2a",
        )
        .await;

        let observation = observe_canonical_block(&endpoints, 42, [9; 32])
            .await
            .unwrap();

        assert_eq!(observation.extrinsic_index(), None);
    }

    #[tokio::test]
    async fn canonical_block_reports_unavailable_null_result() {
        let endpoints = unavailable_block_endpoints(42).await;

        let error = observe_canonical_block(&endpoints, 42, [9; 32])
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::BlockUnavailable(42),
        ));
    }

    #[tokio::test]
    async fn finalized_transaction_location_waits_then_binds_exact_membership() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = BTreeMap::from([(
            41,
            vec![
                format!("0x{}", hex::encode(extrinsic(&[5, 6]))),
                format!("0x{}", hex::encode(&target)),
            ],
        )]);
        let (pending_endpoints, pending_capabilities, _) =
            recovery_endpoints(40, None, blocks.clone(), vec![]).await;

        let pending = observe_finalized_transaction_location(
            &pending_endpoints,
            &pending_capabilities,
            target_hash,
            41,
        )
        .await
        .unwrap();

        assert!(matches!(
            pending,
            DeepXFinalizedTransactionLocationObservation::Pending(checkpoint)
                if checkpoint.block_number() == 40
        ));

        let (endpoints, capabilities, _) = recovery_endpoints(42, None, blocks, vec![]).await;
        let finalized =
            observe_finalized_transaction_location(&endpoints, &capabilities, target_hash, 41)
                .await
                .unwrap();
        let DeepXFinalizedTransactionLocationObservation::Finalized(location) = finalized else {
            panic!("expected finalized transaction location");
        };

        assert_eq!(location.extrinsic_hash(), target_hash);
        assert_eq!(location.block_number(), 41);
        assert_eq!(
            location.block_hash(),
            hex::decode_array(block_hash(41).trim_start_matches("0x")).unwrap(),
        );
        assert_eq!(location.extrinsic_index(), 1);
    }

    #[tokio::test]
    async fn finalized_transaction_location_rejects_absent_target() {
        let blocks = BTreeMap::from([(
            41,
            vec![format!("0x{}", hex::encode(extrinsic(&[1, 2, 3])))],
        )]);
        let (endpoints, capabilities, _) = recovery_endpoints(42, None, blocks, vec![]).await;

        let result =
            observe_finalized_transaction_location(&endpoints, &capabilities, [9; 32], 41).await;

        assert!(matches!(
            result,
            Err(DeepXTransactionWatchError::FinalizedExtrinsicUnavailable(
                41
            )),
        ));
    }

    #[tokio::test]
    async fn malformed_pool_entry_prevents_absence_evidence() {
        let endpoints = endpoints(vec!["not-hex".to_string()], "0x2a").await;

        let error = observe_submission_pool(&endpoints, [9; 32])
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::InvalidExtrinsic {
                evidence_source: "submission pool",
                index: 0,
            },
        ));
    }

    #[tokio::test]
    async fn malformed_scale_length_prevents_absence_evidence() {
        let endpoints = endpoints(vec!["0x100102".to_string()], "0x2a").await;

        let error = observe_submission_pool(&endpoints, [9; 32])
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::InvalidExtrinsic {
                evidence_source: "submission pool",
                index: 0,
            },
        ));
    }

    #[tokio::test]
    async fn block_number_mismatch_is_rejected() {
        let endpoints = endpoints(vec![], "0x2b").await;

        let error = observe_canonical_block(&endpoints, 42, [9; 32])
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::BlockNumberMismatch {
                requested: 42,
                received: 43,
            },
        ));
    }

    #[tokio::test]
    async fn duplicate_target_extrinsic_is_rejected() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let encoded = format!("0x{}", hex::encode(target));
        let endpoints = endpoints(vec![encoded.clone(), encoded], "0x2a").await;

        let error = observe_canonical_block(&endpoints, 42, target_hash)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::DuplicateExtrinsic {
                evidence_source: "canonical block",
            },
        ));
    }

    #[tokio::test]
    async fn pool_membership_requires_exact_extrinsic_hash() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let endpoints = endpoints(
            vec![
                format!("0x{}", hex::encode(extrinsic(&[5, 6]))),
                format!("0x{}", hex::encode(target)),
            ],
            "0x2a",
        )
        .await;

        assert_eq!(
            observe_submission_pool(&endpoints, target_hash)
                .await
                .unwrap(),
            DeepXPoolObservation::Present,
        );
    }

    #[tokio::test]
    async fn finalized_recovery_scan_requires_action_after_non_atomic_pool_absence() {
        let (endpoints, capabilities, _) =
            recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;

        let collection = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            [9; 32],
        )
        .await
        .unwrap();

        let DeepXFinalizedRecoveryCollection::Scan(scan) = collection else {
            panic!("expected a completed recovery scan");
        };
        assert_eq!(scan.classify(), DeepXRecoveryDecision::ActionRequired);
    }

    #[tokio::test]
    async fn finalized_recovery_scan_rejects_excessive_history_before_block_reads() {
        let (endpoints, capabilities, post_checkpoint_requests) =
            recovery_endpoints(1_040, None, BTreeMap::new(), vec![]).await;

        let error = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            100,
            [9; 32],
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::ScanPlan(DeepXRecoveryScanPlanError::ScanLimitExceeded {
                missing_blocks: 1_001,
                max_blocks: 1_000,
            }),
        ));
        assert_eq!(post_checkpoint_requests.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn finalized_recovery_scan_stops_when_event_evidence_is_required() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = BTreeMap::from([(41, vec![format!("0x{}", hex::encode(target))])]);
        let (endpoints, capabilities, _) = recovery_endpoints(42, None, blocks, vec![]).await;

        let error = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            target_hash,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::EventEvidenceUnavailable {
                block_number: 41,
                extrinsic_index: 0,
            },
        ));
    }

    #[tokio::test]
    async fn finalized_recovery_scan_verifies_events_at_exact_canonical_block() {
        let snapshot = runtime_snapshot();
        let subaccount = [42; 20];
        let identity = cancel_identity(subaccount, 9001, false);
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = BTreeMap::from([(41, vec![format!("0x{}", hex::encode(target))])]);
        let events = system_events(&[
            cancel_event_record(&snapshot, 0, subaccount, 9001, 0),
            dispatch_event_record(&snapshot, 0, true),
        ]);
        let event_storage = BTreeMap::from([(41, format!("0x{}", hex::encode(events)))]);
        let (endpoints, capabilities, _) =
            recovery_endpoints_with_events(42, None, blocks, vec![], event_storage).await;

        let collection = collect_finalized_recovery_scan_with_event_evidence(
            &endpoints,
            &capabilities,
            &snapshot,
            &identity,
            DeepXFinalizedRecoveryCheckpoint::new(39, decode_hash(&block_hash(39)).unwrap()),
            2,
            target_hash,
        )
        .await
        .unwrap();

        let DeepXFinalizedRecoveryCollection::Scan(scan) = collection else {
            panic!("expected a completed recovery scan");
        };
        assert_eq!(
            scan.classify(),
            DeepXRecoveryDecision::FinalizedInclusion(inclusion(
                decode_hash(&block_hash(41)).unwrap(),
                41,
                0,
            )),
        );
    }

    #[tokio::test]
    async fn finalized_recovery_scan_reports_up_to_date_checkpoint() {
        let (endpoints, capabilities, _) =
            recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;

        let collection = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            42,
            decode_hash(&block_hash(42)).unwrap(),
            2,
            [9; 32],
        )
        .await
        .unwrap();

        assert_eq!(
            collection,
            DeepXFinalizedRecoveryCollection::UpToDate(DeepXFinalizedRecoveryCheckpoint {
                block_number: 42,
                block_hash: decode_hash(&block_hash(42)).unwrap(),
            }),
        );
    }

    #[rstest::rstest]
    #[case("success")]
    #[case("failed")]
    #[case("identity")]
    #[case("runtime")]
    #[case("fast")]
    #[case("checkpoint")]
    #[case("missing-business")]
    #[case("wrong-index")]
    #[case("absent")]
    #[case("unsupported")]
    #[tokio::test]
    async fn explicit_spot_cancel_recovery_scan(#[case] scenario: &str) {
        let snapshot = runtime_snapshot();
        let mut wire = serde_json::to_value(cancel_identity([0x11; 20], 7, false)).unwrap();
        wire["operation"] = json!({"type": "spot-cancel", "subaccount": vec![0x11; 20],
            "pair": vec![0x22; 32], "order_id": 7, "is_buy": true, "fast_cancel": false});
        if scenario == "identity" {
            wire["operation"]["order_id"] = json!(8);
        }
        if scenario == "fast" {
            wire["operation"]["fast_cancel"] = json!(true);
        }
        if scenario == "runtime" {
            wire["runtime"]["spec_version"] = json!(369);
        }
        let mut identity: DeepXTransactionIdentity = serde_json::from_value(wire).unwrap();
        if scenario == "unsupported" {
            identity = cancel_identity([0x11; 20], 7, false);
        }
        let pallet = snapshot.metadata().pallet_by_name("SpotMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|event| event.name == "OrderCancelled")
            .unwrap();
        let mut record = Phase::ApplyExtrinsic(u32::from(scenario != "wrong-index")).encode();
        record.extend([pallet.index(), event.index]);
        let values = [
            ScaleValue::from_bytes([0x22; 32]),
            ScaleValue::u128(7),
            ScaleValue::from_bytes([0x11; 20]),
            ScaleValue::bool(true),
            ScaleValue::unnamed_variant("UserCanceled", []),
        ];
        for (field, value) in event.fields.iter().zip(values) {
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        let mut records = vec![dispatch_event_record(&snapshot, 1, scenario != "failed")];
        if scenario != "failed" && scenario != "missing-business" {
            records.push(record);
        }
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = if scenario == "absent" {
            BTreeMap::new()
        } else {
            BTreeMap::from([(
                41,
                vec!["0x0400".to_string(), format!("0x{}", hex::encode(target))],
            )])
        };
        let storage = BTreeMap::from([(41, format!("0x{}", hex::encode(system_events(&records))))]);
        let (endpoints, capabilities, _) =
            recovery_endpoints_with_events(42, None, blocks, vec![], storage).await;
        let result = collect_finalized_spot_cancel_recovery_scan(
            &endpoints,
            &capabilities,
            &snapshot,
            &identity,
            DeepXFinalizedRecoveryCheckpoint::new(
                39,
                if scenario == "checkpoint" {
                    [0; 32]
                } else {
                    decode_hash(&block_hash(39)).unwrap()
                },
            ),
            2,
            target_hash,
        )
        .await;
        match scenario {
            "success" | "failed" => {
                let DeepXFinalizedRecoveryCollection::Scan(scan) = result.unwrap() else {
                    panic!("expected scan")
                };
                let DeepXRecoveryDecision::FinalizedInclusion(inclusion) = scan.classify() else {
                    panic!("expected inclusion")
                };
                assert_eq!(
                    inclusion.block_hash(),
                    decode_hash(&block_hash(41)).unwrap()
                );
                assert_eq!(inclusion.block_number(), 41);
                assert_eq!(inclusion.extrinsic_index(), 1);
                assert_eq!(
                    inclusion.outcome(),
                    if scenario == "success" {
                        DeepXInclusionOutcome::Success
                    } else {
                        DeepXInclusionOutcome::Failed
                    }
                );
            }
            "absent" => {
                let DeepXFinalizedRecoveryCollection::Scan(scan) = result.unwrap() else {
                    panic!("expected scan")
                };
                assert_eq!(scan.classify(), DeepXRecoveryDecision::ActionRequired);
            }
            "checkpoint" => assert!(matches!(
                result,
                Err(DeepXTransactionWatchError::RecoveryCheckpointMismatch { .. })
            )),
            _ => assert!(matches!(
                result,
                Err(DeepXTransactionWatchError::SpotEventVerification(_))
            )),
        }
    }

    #[tokio::test]
    async fn explicit_spot_place_recovery_scan() {
        let snapshot = runtime_snapshot();
        let identity = spot_place_identity(true);
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = BTreeMap::from([(
            41,
            vec!["0x0400".to_string(), format!("0x{}", hex::encode(&target))],
        )]);
        let events = system_events(&[
            dispatch_event_record(&snapshot, 1, true),
            spot_place_event_record(&snapshot, 1, &identity),
        ]);
        let storage = BTreeMap::from([(41, format!("0x{}", hex::encode(events)))]);
        let (endpoints, capabilities, _) =
            recovery_endpoints_with_events(42, None, blocks, vec![], storage).await;

        let collection = collect_finalized_spot_place_recovery_scan(
            &endpoints,
            &capabilities,
            &snapshot,
            &identity,
            DeepXFinalizedRecoveryCheckpoint::new(39, decode_hash(&block_hash(39)).unwrap()),
            2,
            target_hash,
        )
        .await
        .unwrap();
        let DeepXFinalizedRecoveryCollection::Scan(scan) = collection else {
            panic!("expected scan")
        };
        assert_eq!(
            scan.classify(),
            DeepXRecoveryDecision::FinalizedInclusion(inclusion(
                decode_hash(&block_hash(41)).unwrap(),
                41,
                1,
            ))
        );

        assert!(matches!(
            collect_finalized_spot_place_recovery_scan(
                &endpoints,
                &capabilities,
                &snapshot,
                &cancel_identity([0x11; 20], 7, false),
                DeepXFinalizedRecoveryCheckpoint::new(39, decode_hash(&block_hash(39)).unwrap()),
                2,
                target_hash,
            )
            .await,
            Err(DeepXTransactionWatchError::SpotPlaceEventVerification(
                DeepXSpotPlaceEventVerificationError::UnsupportedOperation
            ))
        ));
    }

    #[tokio::test]
    async fn finalized_recovery_scan_rejects_conflicting_up_to_date_hash() {
        let conflicting_hash = block_hash(43);
        let (endpoints, capabilities, post_checkpoint_requests) =
            recovery_endpoints(42, Some(conflicting_hash), BTreeMap::new(), vec![]).await;

        let error = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            42,
            decode_hash(&block_hash(42)).unwrap(),
            2,
            [9; 32],
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::RecoveryCheckpointMismatch {
                block_number: 42,
                expected,
                received,
            } if expected == decode_hash(&block_hash(42)).unwrap()
                && received == decode_hash(&block_hash(43)).unwrap()
        ));
        assert_eq!(post_checkpoint_requests.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn finalized_recovery_scan_preserves_pool_acceptance() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let pool = vec![format!("0x{}", hex::encode(target))];
        let (endpoints, capabilities, _) =
            recovery_endpoints(42, None, BTreeMap::new(), pool).await;

        let collection = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            target_hash,
        )
        .await
        .unwrap();

        let DeepXFinalizedRecoveryCollection::Scan(scan) = collection else {
            panic!("expected a completed recovery scan");
        };
        assert_eq!(scan.classify(), DeepXRecoveryDecision::PoolAccepted);
    }

    #[tokio::test]
    async fn finalized_recovery_scan_queries_node_that_accepted_submission() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let submission_pool = vec![format!("0x{}", hex::encode(target))];
        let (endpoints, capabilities) = split_recovery_endpoints(submission_pool, vec![]).await;

        let collection = collect_finalized_recovery_scan(
            &endpoints,
            &capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            target_hash,
        )
        .await
        .unwrap();

        let DeepXFinalizedRecoveryCollection::Scan(scan) = collection else {
            panic!("expected a completed recovery scan");
        };
        assert_eq!(scan.classify(), DeepXRecoveryDecision::PoolAccepted);
    }

    #[tokio::test]
    async fn reorganization_observation_binds_recorded_height_and_extrinsic_hash() {
        let target = extrinsic(&[1, 2, 3, 4]);
        let target_hash = BlakeTwo256.hash(&target).0;
        let blocks = BTreeMap::from([(42, vec![format!("0x{}", hex::encode(target))])]);
        let (endpoints, capabilities, _) = recovery_endpoints(42, None, blocks, vec![]).await;
        let recorded = inclusion(decode_hash(&block_hash(42)).unwrap(), 42, 0);

        assert_eq!(
            observe_reorganization(&endpoints, &capabilities, target_hash, recorded)
                .await
                .unwrap(),
            DeepXReorganizationDecision::Canonical,
        );
        assert_eq!(
            observe_reorganization(&endpoints, &capabilities, [9; 32], recorded)
                .await
                .unwrap(),
            DeepXReorganizationDecision::ActionRequired,
        );
    }

    #[tokio::test]
    async fn reorganization_observation_rejects_capabilities_from_other_endpoints() {
        let (endpoints, _, _) = recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;
        let (_, other_capabilities, _) =
            recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;
        let recorded = inclusion(decode_hash(&block_hash(42)).unwrap(), 42, 0);

        let error = observe_reorganization(&endpoints, &other_capabilities, [9; 32], recorded)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::CapabilitiesMismatch(DeepXRpcRole::Watch),
        ));
    }

    #[tokio::test]
    async fn finalized_recovery_scan_rejects_capabilities_from_other_endpoints() {
        let (endpoints, _, _) = recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;
        let (_, other_capabilities, _) =
            recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;

        let error = collect_finalized_recovery_scan(
            &endpoints,
            &other_capabilities,
            39,
            decode_hash(&block_hash(39)).unwrap(),
            2,
            [9; 32],
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::CapabilitiesMismatch(DeepXRpcRole::Recovery),
        ));
    }

    #[tokio::test]
    async fn finalized_recovery_scan_rejects_changed_checkpoint_before_scanning() {
        let (endpoints, capabilities, post_checkpoint_requests) =
            recovery_endpoints(42, None, BTreeMap::new(), vec![]).await;

        let error =
            collect_finalized_recovery_scan(&endpoints, &capabilities, 39, [7; 32], 2, [9; 32])
                .await
                .unwrap_err();

        assert!(matches!(
            error,
            DeepXTransactionWatchError::RecoveryCheckpointMismatch {
                block_number: 39,
                expected,
                received,
            } if expected == [7; 32] && received == decode_hash(&block_hash(39)).unwrap(),
        ));
        assert_eq!(post_checkpoint_requests.load(Ordering::Relaxed), 0);
    }

    #[rstest]
    #[case(0)]
    #[case(7)]
    fn finalized_spot_match_decodes_approved_event_and_exact_account_trade(#[case] buy_id: u64) {
        let snapshot = read_only_runtime_snapshot();
        let pallet = snapshot.metadata().pallet_by_name("SpotMarket").unwrap();
        let event = pallet
            .event_variants()
            .unwrap()
            .iter()
            .find(|variant| variant.name == "OrderMatched")
            .unwrap();
        assert_eq!(event.fields.len(), 13);
        let u256 = |value: u128| {
            let mut bytes = [0; 32];
            bytes[..16].copy_from_slice(&value.to_le_bytes());
            u256_value(bytes)
        };
        let mut record = Phase::ApplyExtrinsic(3).encode();
        record.extend([pallet.index(), event.index]);
        for field in &event.fields {
            let value = match field.name.as_deref().unwrap() {
                "pair" => ScaleValue::from_bytes([0x5a; 32]),
                "buyer" => ScaleValue::from_bytes([0x11; 20]),
                "buy_id" => ScaleValue::u128(u128::from(buy_id)),
                "seller" => ScaleValue::from_bytes([0x22; 20]),
                "sell_id" => ScaleValue::u128(8),
                "trade_amount_u" => u256(50_000_000),
                "trade_amount_b" => u256(1_000_000_000_000_000_000),
                "taker" => ScaleValue::unnamed_variant("Buyer", []),
                "taker_fee" => ScaleValue::i128(10_000_000_000_000_000),
                "taker_fee_asset" => ScaleValue::from_bytes(b"ETH"),
                "maker_fee" => ScaleValue::i128(100_000),
                "maker_fee_asset" => ScaleValue::from_bytes(b"USDC"),
                "price" => u256(50_000_000),
                unknown => panic!("unexpected Spot match field {unknown}"),
            };
            subxt_core::ext::scale_value::scale::encode_as_type(
                &value,
                field.ty.id,
                snapshot.metadata().types(),
                &mut record,
            )
            .unwrap();
        }
        Vec::<[u8; 32]>::new().encode_to(&mut record);
        let events = system_events(&[record]);
        let trades = decode_finalized_spot_trade_events_with_metadata(
            snapshot.interfaces(),
            snapshot.metadata(),
            [3; 32],
            42,
            &events,
        )
        .unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].event_index, 0);
        assert_eq!(
            trades[0].trade_amount_b[..16],
            1_000_000_000_000_000_000_u128.to_le_bytes()
        );
        let account_trade = DeepXSpotAccountTradeRecord {
            id: 42_000_000,
            order_id: buy_id.to_string(),
            order_side: "Buy".to_string(),
            pair: format!("0x{}", hex::encode([0x5a; 32])),
            pair_name: "ETH/USDC".to_string(),
            base_amount: "1".parse().unwrap(),
            quote_amount: "50".parse().unwrap(),
            price: "50".parse().unwrap(),
            fee: "-0.01".parse().unwrap(),
            fee_asset: "ETH".to_string(),
            taker: "Buyer".to_string(),
            created_at: "2026-06-18T15:13:14.739Z".to_string(),
        };
        assert_eq!(
            match_finalized_spot_account_trade(
                &trades,
                &account_trade,
                [0x11; 20],
                18,
                6,
                "ETH",
                "USDC",
            )
            .unwrap(),
            &trades[0]
        );
        let mut seller_trade = account_trade.clone();
        seller_trade.order_id = "8".to_string();
        seller_trade.order_side = "Sell".to_string();
        seller_trade.fee = "-0.1".parse().unwrap();
        seller_trade.fee_asset = "USDC".to_string();
        assert_eq!(
            match_finalized_spot_account_trade(
                &trades,
                &seller_trade,
                [0x22; 20],
                18,
                6,
                "ETH",
                "USDC",
            )
            .unwrap(),
            &trades[0]
        );
        seller_trade.fee_asset = "ETH".to_string();
        assert!(matches!(
            match_finalized_spot_account_trade(
                &trades,
                &seller_trade,
                [0x22; 20],
                18,
                6,
                "ETH",
                "USDC"
            ),
            Err(DeepXSpotTradeMatchError::InvalidInput)
        ));
        let mut conflicting = account_trade;
        conflicting.quote_amount = "49.999999".parse().unwrap();
        assert!(matches!(
            match_finalized_spot_account_trade(
                &trades,
                &conflicting,
                [0x11; 20],
                18,
                6,
                "ETH",
                "USDC"
            ),
            Err(DeepXSpotTradeMatchError::NotFound)
        ));
        conflicting.quote_amount = "50".parse().unwrap();
        conflicting.id += 1;
        assert!(matches!(
            match_finalized_spot_account_trade(
                &trades,
                &conflicting,
                [0x11; 20],
                18,
                6,
                "ETH",
                "USDC"
            ),
            Err(DeepXSpotTradeMatchError::NotFound)
        ));
        let mut truncated = events;
        truncated.pop();
        assert!(
            decode_finalized_spot_trade_events_with_metadata(
                snapshot.interfaces(),
                snapshot.metadata(),
                [3; 32],
                42,
                &truncated,
            )
            .is_err()
        );
    }
}
