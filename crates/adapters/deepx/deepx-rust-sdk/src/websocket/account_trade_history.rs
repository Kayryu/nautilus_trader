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

//! Perpetual account trade-history subscription and acknowledgement protocol.

use std::{borrow::Borrow, collections::HashSet};

use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror::Error;

use super::{account::DeepXWsUserBalancesRequest, public::DeepXWsPerpMarket};
use crate::{
    decimal::exact_decimal,
    parse::parse_canonical_order_id,
    query::DeepXPerpAccountTradesRequest,
    rest::{DeepXAccountPage, models::DeepXPerpAccountTradeRecord},
    rpc::{
        events::DeepXFinalizedPerpTrade,
        trade_match::{DeepXPerpTradeMatchError, match_finalized_perp_account_trade},
    },
};

/// Failures matching a perpetual WebSocket batch to REST account trades.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeepXWsPerpTradeHistoryRestMatchError {
    /// The REST request did not bind the exact WebSocket subaccount and market scope.
    #[error("DeepX trade-history REST request conflicts with WebSocket scope")]
    RequestScope,
    /// The WebSocket timestamp could not be compared exactly.
    #[error("invalid DeepX trade-history timestamp")]
    InvalidTimestamp,
    /// No supplied REST record agreed with one WebSocket item.
    #[error("DeepX incremental trade has no matching REST account record")]
    NotFound,
    /// Two supplied REST records agree with one WebSocket item.
    #[error("DeepX incremental trade matches multiple REST account records")]
    Ambiguous,
    /// Two WebSocket items would consume the same REST trade identity.
    #[error("DeepX incremental trade batch reuses one REST trade identity")]
    DuplicateTrade,
}

/// Failures matching one incremental batch through REST to finalized perpetual events.
#[derive(Debug, Error)]
pub enum DeepXWsPerpTradeHistoryMatchError {
    /// The REST request did not bind the exact WebSocket subaccount and market scope.
    #[error("DeepX trade-history REST request conflicts with WebSocket scope")]
    RequestScope,
    /// The WebSocket timestamp could not be compared exactly.
    #[error("invalid DeepX trade-history timestamp")]
    InvalidTimestamp,
    /// No supplied REST record agreed with one WebSocket item.
    #[error("DeepX incremental trade has no matching REST account record")]
    NotFound,
    /// Two supplied REST records agree with one WebSocket item.
    #[error("DeepX incremental trade matches multiple REST account records")]
    Ambiguous,
    /// Two WebSocket items would consume the same REST trade identity.
    #[error("DeepX incremental trade batch reuses one REST trade identity")]
    DuplicateTrade,
    /// The WebSocket batch subaccount cannot be bound to the supplied chain account.
    #[error("DeepX trade-history subaccount conflicts with finalized account")]
    FinalizedAccountScope,
    /// One REST record could not be matched to the supplied finalized event evidence.
    #[error(transparent)]
    Finalized(#[from] DeepXPerpTradeMatchError),
}

impl From<DeepXWsPerpTradeHistoryRestMatchError> for DeepXWsPerpTradeHistoryMatchError {
    fn from(value: DeepXWsPerpTradeHistoryRestMatchError) -> Self {
        match value {
            DeepXWsPerpTradeHistoryRestMatchError::RequestScope => Self::RequestScope,
            DeepXWsPerpTradeHistoryRestMatchError::InvalidTimestamp => Self::InvalidTimestamp,
            DeepXWsPerpTradeHistoryRestMatchError::NotFound => Self::NotFound,
            DeepXWsPerpTradeHistoryRestMatchError::Ambiguous => Self::Ambiguous,
            DeepXWsPerpTradeHistoryRestMatchError::DuplicateTrade => Self::DuplicateTrade,
        }
    }
}

/// Independently validated account and market units for finalized trade matching.
#[derive(Clone, Copy, Debug)]
pub struct DeepXWsPerpFinalizedTradeScope<'a> {
    /// AccountId20 bound to the WebSocket batch and REST request.
    pub subaccount: [u8; 20],
    /// Base asset decimal places used by runtime trade size.
    pub base_decimal: u8,
    /// Quote asset decimal places used by runtime trade fee.
    pub quote_decimal: u8,
    /// Quote asset symbol carried by the runtime fee asset.
    pub quote_symbol: &'a str,
}

/// One market-bound, address-scoped perpetual trade-history subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsPerpTradeHistoryRequest {
    market_id: u16,
    address: String,
    expected_subaccounts: Option<HashSet<String>>,
}

impl DeepXWsPerpTradeHistoryRequest {
    /// Creates a read-only subscription for one subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero market ID or invalid AccountId20.
    pub fn new(market_id: u16, subaccount: &str) -> Result<Self> {
        ensure!(market_id != 0, "DeepX perpetual market ID must be positive");
        let address = DeepXWsUserBalancesRequest::new(subaccount)?
            .subaccount()
            .to_string();
        Ok(Self {
            market_id,
            address,
            expected_subaccounts: None,
        })
    }

    /// Creates a wallet subscription bound to an independently read subaccount directory.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, duplicated, or invalid subaccount set.
    pub fn for_wallet(market_id: u16, wallet: &str, subaccounts: &[String]) -> Result<Self> {
        let mut request = Self::new(market_id, wallet)?;
        ensure!(
            !subaccounts.is_empty(),
            "DeepX perpetual wallet trade subscription requires subaccounts"
        );
        let mut expected = HashSet::with_capacity(subaccounts.len());
        for subaccount in subaccounts {
            let address = DeepXWsUserBalancesRequest::new(subaccount)?
                .subaccount()
                .to_string();
            ensure!(
                expected.insert(address),
                "duplicate DeepX perpetual wallet trade subaccount"
            );
        }
        request.expected_subaccounts = Some(expected);
        Ok(request)
    }

    /// Returns the subscribed perpetual market ID.
    #[must_use]
    pub const fn market_id(&self) -> u16 {
        self.market_id
    }

    /// Returns whether the request subscribes to a wallet's subaccount directory.
    #[must_use]
    pub const fn is_wallet(&self) -> bool {
        self.expected_subaccounts.is_some()
    }

    /// Returns whether a canonical subaccount belongs to the subscribed scope.
    #[must_use]
    pub fn accepts_subaccount(&self, subaccount: &str) -> bool {
        self.expected_subaccounts
            .as_ref()
            .map_or(subaccount == self.address, |expected| {
                expected.contains(subaccount)
            })
    }

    /// Serializes the observed read-only subscription request.
    ///
    /// # Errors
    ///
    /// Returns an error if the validated request cannot be serialized.
    pub fn to_text(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Market {
            #[serde(rename = "type")]
            kind: &'static str,
            id: u16,
        }
        #[derive(Serialize)]
        struct Subscription<'a> {
            channel: &'static str,
            address: &'a str,
            #[serde(rename = "addressType", skip_serializing_if = "Option::is_none")]
            address_type: Option<&'static str>,
        }
        #[derive(Serialize)]
        struct Options {
            compress: bool,
        }
        #[derive(Serialize)]
        struct Request<'a> {
            action: &'static str,
            market: Market,
            subscriptions: [Subscription<'a>; 1],
            options: Options,
        }

        Ok(serde_json::to_string(&Request {
            action: "subscribe",
            market: Market {
                kind: "perp",
                id: self.market_id,
            },
            subscriptions: [Subscription {
                channel: "user_perp_trade_history",
                address: &self.address,
                address_type: self.expected_subaccounts.as_ref().map(|_| "wallet"),
            }],
            options: Options { compress: false },
        })?)
    }

    /// Validates the exact market and account scope of an acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error for a malformed or foreign acknowledgement.
    pub fn validate_ack(&self, text: &str) -> Result<()> {
        #[derive(Deserialize)]
        struct Subscription {
            channel: String,
            address: String,
        }
        #[derive(Deserialize)]
        struct Ack {
            #[serde(rename = "type")]
            kind: String,
            market: DeepXWsPerpMarket,
            subscriptions: Vec<Subscription>,
        }

        let ack: Ack = serde_json::from_str(text)?;
        ensure!(
            ack.kind == "subscribed",
            "unexpected DeepX trade-history frame"
        );
        ensure!(
            ack.market.kind == "perp" && ack.market.id == self.market_id,
            "foreign DeepX perpetual trade-history market"
        );
        let mut observed = HashSet::with_capacity(ack.subscriptions.len());
        for subscription in ack.subscriptions {
            ensure!(
                subscription.channel == "user_perp_trade_history"
                    && observed.insert(subscription.address.to_ascii_lowercase()),
                "foreign or duplicate DeepX trade-history subscription"
            );
        }
        if let Some(expected) = &self.expected_subaccounts {
            ensure!(
                &observed == expected,
                "DeepX wallet trade-history acknowledgement differs from directory"
            );
        } else {
            ensure!(
                observed.len() == 1 && observed.contains(&self.address),
                "foreign DeepX trade-history subscription"
            );
        }
        Ok(())
    }
}

/// One typed but unconfirmed incremental perpetual trade observation.
#[derive(Debug)]
pub struct DeepXWsPerpTradeHistoryItem {
    /// Venue trade ID, absent from the documented WebSocket example.
    pub id: Option<u64>,
    /// Perpetual market ID bound to the enclosing subscription.
    pub market_id: u64,
    /// Exact decimal venue order ID.
    pub order_id: String,
    /// Venue long-side flag without framework-side interpretation.
    pub is_long: bool,
    /// Venue taker role.
    pub taker: String,
    /// Exact execution price.
    pub price: Decimal,
    /// Exact execution quantity.
    pub size: Decimal,
    /// Exact leverage.
    pub leverage: Decimal,
    /// Venue creation timestamp, validated as RFC 3339.
    pub created_at: String,
    /// Venue fill-direction value.
    pub filled_direction: String,
    /// Exact signed venue fee.
    pub fee: Decimal,
    /// Venue fee asset when supplied.
    pub fee_asset: Option<String>,
    raw: Box<RawValue>,
}

impl DeepXWsPerpTradeHistoryItem {
    /// Parses an exact trade item for the subscribed perpetual market.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed or invalid venue fields or a foreign market ID.
    pub fn parse(raw: Box<RawValue>, market_id: u16) -> Result<Self> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct WireTrade {
            id: Option<u64>,
            market_id: u64,
            order_id: String,
            is_long: bool,
            taker: String,
            #[serde(deserialize_with = "exact_decimal::deserialize")]
            price: Decimal,
            #[serde(deserialize_with = "exact_decimal::deserialize")]
            size: Decimal,
            #[serde(deserialize_with = "exact_decimal::deserialize")]
            leverage: Decimal,
            created_at: String,
            filled_direction: String,
            #[serde(deserialize_with = "exact_decimal::deserialize")]
            fee: Decimal,
            fee_asset: Option<String>,
        }

        let trade: WireTrade = serde_json::from_str(raw.get())?;
        ensure!(
            parse_canonical_order_id(&trade.order_id).is_some(),
            "invalid DeepX trade-history order ID"
        );
        let created_at = trade.created_at.parse::<jiff::Timestamp>()?;
        ensure!(
            trade.market_id == u64::from(market_id)
                && trade.id.is_none_or(|id| id != 0)
                && matches!(trade.taker.as_str(), "Buyer" | "Seller")
                && matches!(trade.filled_direction.as_str(), "Long" | "Short" | "Both")
                && trade.price > Decimal::ZERO
                && trade.size >= Decimal::ZERO
                && trade.leverage > Decimal::ZERO
                && created_at.as_nanosecond() >= 0,
            "foreign or invalid DeepX perpetual trade-history item"
        );
        Ok(Self {
            id: trade.id,
            market_id: trade.market_id,
            order_id: trade.order_id,
            is_long: trade.is_long,
            taker: trade.taker,
            price: trade.price,
            size: trade.size,
            leverage: trade.leverage,
            created_at: trade.created_at,
            filled_direction: trade.filled_direction,
            fee: trade.fee,
            fee_asset: trade.fee_asset,
            raw,
        })
    }

    /// Returns the complete source item with numeric JSON lexemes preserved.
    #[must_use]
    pub fn raw(&self) -> &RawValue {
        &self.raw
    }
}

/// One address-bound incremental perpetual trade batch.
#[derive(Debug)]
pub struct DeepXWsPerpTradeHistoryBatch {
    subaccount: String,
    items: Vec<DeepXWsPerpTradeHistoryItem>,
    timestamp_ms: u64,
}

impl DeepXWsPerpTradeHistoryBatch {
    /// Returns the acknowledged subaccount carrying this batch.
    #[must_use]
    pub fn subaccount(&self) -> &str {
        &self.subaccount
    }

    /// Returns typed trade observations retaining their original JSON.
    #[must_use]
    pub fn items(&self) -> &[DeepXWsPerpTradeHistoryItem] {
        &self.items
    }

    /// Returns the venue frame timestamp in Unix milliseconds.
    #[must_use]
    pub const fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }
}

/// Matches every incremental perpetual trade to one validated REST account record atomically.
///
/// The supplied records are mutable candidate evidence. A match does not establish chain finality,
/// stream continuity, or permission to dispatch a fill.
///
/// # Errors
///
/// Returns an error for scope conflict, invalid timestamps, missing or ambiguous records, or a
/// duplicate matched trade identity.
pub fn match_perp_trade_history_batch_to_rest<'a, R>(
    batch: &DeepXWsPerpTradeHistoryBatch,
    request: &DeepXPerpAccountTradesRequest,
    records: &'a [R],
) -> Result<Vec<&'a R>, DeepXWsPerpTradeHistoryRestMatchError>
where
    R: Borrow<DeepXPerpAccountTradeRecord>,
{
    use DeepXWsPerpTradeHistoryRestMatchError as Error;

    if request.validate().is_err()
        || !request.subaccount.eq_ignore_ascii_case(batch.subaccount())
        || request
            .market_id
            .is_some_and(|market_id| batch.items().iter().any(|item| item.market_id != market_id))
        || request
            .order_id
            .as_ref()
            .is_some_and(|order_id| batch.items().iter().any(|item| item.order_id != *order_id))
        || request
            .is_long
            .is_some_and(|is_long| batch.items().iter().any(|item| item.is_long != is_long))
    {
        return Err(Error::RequestScope);
    }
    let mut matched = Vec::with_capacity(batch.items().len());
    let mut trade_ids = HashSet::with_capacity(batch.items().len());

    for item in batch.items() {
        let timestamp = item
            .created_at
            .parse::<jiff::Timestamp>()
            .map_err(|_| Error::InvalidTimestamp)?;
        let timestamp_ms =
            u64::try_from(timestamp.as_millisecond()).map_err(|_| Error::InvalidTimestamp)?;
        if request.start_ms.is_some_and(|start| timestamp_ms < start)
            || request.end_ms.is_some_and(|end| timestamp_ms > end)
        {
            return Err(Error::RequestScope);
        }
        let mut candidates = records.iter().filter(|candidate| {
            let record: &DeepXPerpAccountTradeRecord = (*candidate).borrow();
            record.id != 0
                && item.id.is_none_or(|id| id == record.id)
                && record.market_id == item.market_id
                && record.order_id == item.order_id
                && record.is_long == item.is_long
                && record.taker == item.taker
                && record.price == item.price
                && record.size == item.size
                && record.leverage == item.leverage
                && record.filled_direction == item.filled_direction
                && record.fee == item.fee
                && item
                    .fee_asset
                    .as_ref()
                    .is_none_or(|asset| asset.eq_ignore_ascii_case(&record.fee_asset))
                && record
                    .created_at
                    .parse::<jiff::Timestamp>()
                    .is_ok_and(|value| value == timestamp)
        });
        let record = candidates.next().ok_or(Error::NotFound)?;
        if candidates.next().is_some() {
            return Err(Error::Ambiguous);
        }
        if !trade_ids.insert(<R as Borrow<DeepXPerpAccountTradeRecord>>::borrow(record).id) {
            return Err(Error::DuplicateTrade);
        }
        matched.push(record);
    }
    Ok(matched)
}

/// Matches every WebSocket item through REST to a supplied finalized trade event.
///
/// The caller independently validates the REST response, market scales, and canonical finality.
/// This pure match does not prove stream continuity or REST historical completeness.
///
/// # Errors
///
/// Returns an error if account scope conflicts or either exact match stage fails.
pub fn match_perp_trade_history_batch_to_finalized<'a, 'b, R>(
    batch: &DeepXWsPerpTradeHistoryBatch,
    request: &DeepXPerpAccountTradesRequest,
    records: &'a [R],
    trades: &'b [DeepXFinalizedPerpTrade],
    scope: DeepXWsPerpFinalizedTradeScope<'_>,
) -> Result<Vec<(&'a R, &'b DeepXFinalizedPerpTrade)>, DeepXWsPerpTradeHistoryMatchError>
where
    R: Borrow<DeepXPerpAccountTradeRecord>,
{
    use DeepXWsPerpTradeHistoryMatchError as Error;

    let encoded_subaccount = format!("0x{}", hex::encode(scope.subaccount));
    if !batch.subaccount().eq_ignore_ascii_case(&encoded_subaccount) {
        return Err(Error::FinalizedAccountScope);
    }
    let records = match_perp_trade_history_batch_to_rest(batch, request, records)?;
    records
        .into_iter()
        .map(|record| {
            let trade = match_finalized_perp_account_trade(
                trades,
                record.borrow(),
                scope.subaccount,
                scope.base_decimal,
                scope.quote_decimal,
                scope.quote_symbol,
            )?;
            Ok((record, trade))
        })
        .collect()
}

/// One frame admitted for an exact perpetual trade-history subscription.
#[derive(Debug)]
pub enum DeepXWsPerpTradeHistoryFrame {
    /// The requested market and address set were acknowledged.
    Subscribed,
    /// One typed account-scoped incremental batch, without fill interpretation.
    TradeBatch(DeepXWsPerpTradeHistoryBatch),
    /// Application heartbeat response.
    Pong,
}

impl DeepXWsPerpTradeHistoryFrame {
    /// Parses and binds one documented trade-history envelope to the request.
    ///
    /// Items retain their raw JSON alongside exact typed fields. The documented example omits a
    /// stable venue trade ID, so this parser cannot deduplicate batches or authorize framework
    /// execution events.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, foreign, nonterminal, or unsupported frames.
    pub fn parse(text: &str, request: &DeepXWsPerpTradeHistoryRequest) -> Result<Self> {
        #[derive(Deserialize)]
        struct WireFrame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsPerpMarket>,
            channel: Option<String>,
            data: Option<Box<RawValue>>,
            timestamp: Option<u64>,
        }
        #[derive(Deserialize)]
        struct Data {
            address: String,
            trades: DeepXAccountPage<Box<RawValue>>,
        }
        let wire: WireFrame = serde_json::from_str(text)?;
        match wire.kind.as_str() {
            "pong" => Ok(Self::Pong),
            "subscribed" => {
                request.validate_ack(text)?;
                Ok(Self::Subscribed)
            }
            "data" => {
                let market = wire.market.context("missing DeepX trade-history market")?;
                ensure!(
                    market.kind == "perp" && market.id == request.market_id(),
                    "foreign DeepX perpetual trade-history market"
                );
                ensure!(
                    wire.channel.as_deref() == Some("user_perp_trade_history"),
                    "foreign DeepX perpetual trade-history channel"
                );
                let data: Data = serde_json::from_str(
                    wire.data.context("missing DeepX trade-history data")?.get(),
                )?;
                let subaccount = DeepXWsUserBalancesRequest::new(&data.address)?
                    .subaccount()
                    .to_string();
                if request.is_wallet() {
                    ensure!(
                        request.accepts_subaccount(&subaccount),
                        "foreign DeepX wallet trade-history subaccount"
                    );
                } else {
                    ensure!(
                        request.accepts_subaccount(&subaccount),
                        "foreign DeepX trade-history subaccount"
                    );
                }
                ensure!(
                    !data.trades.has_next && data.trades.next_cursor.is_none(),
                    "DeepX incremental trade-history batch has a continuation cursor"
                );
                let mut items = Vec::with_capacity(data.trades.items.len());
                for raw in data.trades.items {
                    items.push(DeepXWsPerpTradeHistoryItem::parse(
                        raw,
                        request.market_id(),
                    )?);
                }
                let timestamp_ms = wire
                    .timestamp
                    .context("missing DeepX trade-history timestamp")?;
                ensure!(
                    timestamp_ms > 0 && i64::try_from(timestamp_ms).is_ok(),
                    "invalid DeepX trade-history timestamp"
                );
                Ok(Self::TradeBatch(DeepXWsPerpTradeHistoryBatch {
                    subaccount,
                    items,
                    timestamp_ms,
                }))
            }
            _ => anyhow::bail!("unsupported DeepX trade-history frame"),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::query::DeepXAccountSortOrder;

    const ADDRESS: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
    const OTHER: &str = "0x977654069311fa41b88f7901dc65fae9a76fd60b";
    const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
    const ACK: &str = r#"{"type":"subscribed","market":{"type":"perp","id":3,"name":"ETH-USDC"},"subscriptions":[{"channel":"user_perp_trade_history","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"},{"channel":"user_perp_trade_history","address":"0x977654069311fa41b88f7901dc65fae9a76fd60b"}],"message":"Successfully subscribed to 2 channels"}"#;
    const DATA: &str = r#"{"type":"data","channel":"user_perp_trade_history","market":{"type":"perp","id":3},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","trades":{"items":[{"marketId":3,"orderId":"87561","isLong":true,"taker":"Buyer","price":2209.7600,"size":0.01,"leverage":10,"createdAt":"2026-02-06T14:52:25.613Z","filledDirection":"Long","fee":0.02}],"nextCursor":null,"hasNext":false}},"timestamp":1791475653766}"#;

    #[rstest]
    fn wallet_ack_matches_exact_directory_and_market() {
        let request = DeepXWsPerpTradeHistoryRequest::for_wallet(
            3,
            WALLET,
            &[ADDRESS.to_string(), OTHER.to_string()],
        )
        .unwrap();
        let encoded: serde_json::Value = serde_json::from_str(&request.to_text().unwrap()).unwrap();
        assert_eq!(
            encoded["subscriptions"][0]["channel"],
            "user_perp_trade_history"
        );
        assert_eq!(encoded["subscriptions"][0]["addressType"], "wallet");
        assert!(request.validate_ack(ACK).is_ok());
        assert!(request.validate_ack(&ACK.replace(OTHER, ADDRESS)).is_err());
        assert!(request.validate_ack(&ACK.replace(OTHER, WALLET)).is_err());
        assert!(
            request
                .validate_ack(&ACK.replace("\"id\":3", "\"id\":4"))
                .is_err()
        );
        assert!(
            request
                .validate_ack(&ACK.replace("user_perp_trade_history", "user_perp_open_orders"))
                .is_err()
        );
        assert!(
            request
                .validate_ack(&ACK.replace("subscribed", "data"))
                .is_err()
        );
    }

    #[rstest]
    fn trade_history_request_rejects_invalid_scope() {
        assert!(DeepXWsPerpTradeHistoryRequest::new(0, ADDRESS).is_err());
        assert!(DeepXWsPerpTradeHistoryRequest::new(3, "invalid").is_err());
        assert!(DeepXWsPerpTradeHistoryRequest::for_wallet(3, WALLET, &[]).is_err());
        assert!(
            DeepXWsPerpTradeHistoryRequest::for_wallet(
                3,
                WALLET,
                &[ADDRESS.to_string(), ADDRESS.to_string()],
            )
            .is_err()
        );
    }

    #[rstest]
    fn documented_incremental_batch_retains_raw_numbers_and_binds_identity() {
        let request = DeepXWsPerpTradeHistoryRequest::for_wallet(
            3,
            WALLET,
            &[ADDRESS.to_string(), OTHER.to_string()],
        )
        .unwrap();
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsPerpTradeHistoryFrame::parse(DATA, &request).unwrap()
        else {
            panic!("expected raw trade batch");
        };
        assert_eq!(batch.subaccount(), ADDRESS);
        assert_eq!(batch.timestamp_ms(), 1_791_475_653_766);
        assert_eq!(batch.items().len(), 1);
        assert_eq!(batch.items()[0].price, Decimal::new(220_976, 2));
        assert_eq!(batch.items()[0].id, None);
        assert_eq!(batch.items()[0].fee_asset, None);
        assert!(batch.items()[0].raw().get().contains("2209.7600"));
        let with_id = DATA.replace(
            "\"marketId\":3",
            "\"id\":187071212000014,\"marketId\":3,\"feeAsset\":\"USDC\"",
        );
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(with_id) =
            DeepXWsPerpTradeHistoryFrame::parse(&with_id, &request).unwrap()
        else {
            panic!("expected trade batch with venue ID");
        };
        assert_eq!(with_id.items()[0].id, Some(187_071_212_000_014));
        assert_eq!(with_id.items()[0].fee_asset.as_deref(), Some("USDC"));
        let first_generated = DATA.replace("\"orderId\":\"87561\"", "\"orderId\":\"0\"");
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(first_generated) =
            DeepXWsPerpTradeHistoryFrame::parse(&first_generated, &request).unwrap()
        else {
            panic!("expected zero-ID trade batch");
        };
        assert_eq!(first_generated.items()[0].order_id, "0");
        for changed in [
            DATA.replace(ADDRESS, WALLET),
            DATA.replace("\"id\":3", "\"id\":4"),
            DATA.replace("\"marketId\":3", "\"marketId\":4"),
            DATA.replace("\"orderId\":\"87561\"", "\"orderId\":\"00\""),
            DATA.replace("\"hasNext\":false", "\"hasNext\":true"),
            DATA.replace("\"timestamp\":1791475653766", "\"timestamp\":0"),
            DATA.replace("\"size\":0.01", "\"size\":-0.01"),
            DATA.replace("\"fee\":0.02", "\"fee\":\"invalid\""),
            DATA.replace("\"taker\":\"Buyer\"", "\"taker\":\"Other\""),
            DATA.replace(
                "\"createdAt\":\"2026-02-06T14:52:25.613Z\"",
                "\"createdAt\":\"invalid\"",
            ),
        ] {
            assert!(DeepXWsPerpTradeHistoryFrame::parse(&changed, &request).is_err());
        }
    }

    #[rstest]
    fn incremental_batch_matches_only_unique_exact_rest_trades() {
        let ws_request = DeepXWsPerpTradeHistoryRequest::new(3, ADDRESS).unwrap();
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsPerpTradeHistoryFrame::parse(DATA, &ws_request).unwrap()
        else {
            panic!("expected trade batch");
        };
        let mut request = DeepXPerpAccountTradesRequest {
            subaccount: ADDRESS.to_string(),
            order_id: None,
            market_id: Some(3),
            is_long: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(100),
        };
        let record = DeepXPerpAccountTradeRecord {
            id: 187_071_212_000_014,
            market_id: 3,
            order_id: "87561".to_string(),
            is_long: true,
            taker: "Buyer".to_string(),
            price: Decimal::new(220_976, 2),
            size: Decimal::new(1, 2),
            leverage: Decimal::TEN,
            created_at: "2026-02-06T14:52:25.613Z".to_string(),
            filled_direction: "Long".to_string(),
            fee: Decimal::new(2, 2),
            fee_asset: String::new(),
        };
        assert_eq!(
            match_perp_trade_history_batch_to_rest(&batch, &request, std::slice::from_ref(&record))
                .unwrap()[0]
                .id,
            record.id,
        );
        let created_ms = u64::try_from(
            record
                .created_at
                .parse::<jiff::Timestamp>()
                .unwrap()
                .as_millisecond(),
        )
        .unwrap();
        request.start_ms = Some(created_ms + 1);
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&batch, &request, std::slice::from_ref(&record)),
            Err(DeepXWsPerpTradeHistoryRestMatchError::RequestScope),
        ));
        request.start_ms = Some(created_ms);
        request.end_ms = Some(created_ms);
        assert_eq!(
            match_perp_trade_history_batch_to_rest(&batch, &request, std::slice::from_ref(&record))
                .unwrap()[0]
                .id,
            record.id,
        );
        request.start_ms = None;
        request.end_ms = Some(created_ms - 1);
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&batch, &request, std::slice::from_ref(&record)),
            Err(DeepXWsPerpTradeHistoryRestMatchError::RequestScope),
        ));
        request.start_ms = Some(created_ms);
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&batch, &request, std::slice::from_ref(&record)),
            Err(DeepXWsPerpTradeHistoryRestMatchError::RequestScope),
        ));
        request.start_ms = None;
        request.end_ms = None;
        let mut with_fee_asset = record.clone();
        with_fee_asset.fee_asset = "USDC".to_string();
        let with_id = DATA.replace(
            "\"marketId\":3",
            "\"id\":187071212000014,\"marketId\":3,\"feeAsset\":\"USDC\"",
        );
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(with_id_batch) =
            DeepXWsPerpTradeHistoryFrame::parse(&with_id, &ws_request).unwrap()
        else {
            panic!("expected trade batch with venue ID and fee asset");
        };
        assert_eq!(
            match_perp_trade_history_batch_to_rest(
                &with_id_batch,
                &request,
                &[with_fee_asset.clone()],
            )
            .unwrap()[0]
                .id,
            record.id,
        );
        let empty_fee_asset = with_id.replace("\"feeAsset\":\"USDC\"", "\"feeAsset\":\"\"");
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(empty_fee_asset_batch) =
            DeepXWsPerpTradeHistoryFrame::parse(&empty_fee_asset, &ws_request).unwrap()
        else {
            panic!("expected trade batch with empty fee asset");
        };
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(
                &empty_fee_asset_batch,
                &request,
                &[with_fee_asset.clone()],
            ),
            Err(DeepXWsPerpTradeHistoryRestMatchError::NotFound),
        ));
        with_fee_asset.id += 1;
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&with_id_batch, &request, &[with_fee_asset]),
            Err(DeepXWsPerpTradeHistoryRestMatchError::NotFound),
        ));
        let mut changed_fee = record.clone();
        changed_fee.fee += Decimal::new(1, 6);
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&batch, &request, &[changed_fee]),
            Err(DeepXWsPerpTradeHistoryRestMatchError::NotFound),
        ));
        let mut different_id = record.clone();
        different_id.id += 1;
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(
                &batch,
                &request,
                &[record.clone(), different_id]
            ),
            Err(DeepXWsPerpTradeHistoryRestMatchError::Ambiguous),
        ));
        let mut repeated: serde_json::Value = serde_json::from_str(DATA).unwrap();
        let item = repeated["data"]["trades"]["items"][0].clone();
        repeated["data"]["trades"]["items"]
            .as_array_mut()
            .unwrap()
            .push(item);
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(repeated) =
            DeepXWsPerpTradeHistoryFrame::parse(&repeated.to_string(), &ws_request).unwrap()
        else {
            panic!("expected repeated trade batch");
        };
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(
                &repeated,
                &request,
                std::slice::from_ref(&record)
            ),
            Err(DeepXWsPerpTradeHistoryRestMatchError::DuplicateTrade),
        ));
        request.subaccount = OTHER.to_string();
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&batch, &request, &[record]),
            Err(DeepXWsPerpTradeHistoryRestMatchError::RequestScope),
        ));
    }

    #[rstest]
    fn incremental_batch_matches_exact_finalized_trade() {
        let ws_request = DeepXWsPerpTradeHistoryRequest::new(3, ADDRESS).unwrap();
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsPerpTradeHistoryFrame::parse(DATA, &ws_request).unwrap()
        else {
            panic!("expected trade batch");
        };
        let request = DeepXPerpAccountTradesRequest {
            subaccount: ADDRESS.to_string(),
            order_id: None,
            market_id: Some(3),
            is_long: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(100),
        };
        let account: [u8; 20] = hex::decode(&ADDRESS[2..]).unwrap().try_into().unwrap();
        let record = DeepXPerpAccountTradeRecord {
            id: 42_000_001,
            market_id: 3,
            order_id: "87561".to_string(),
            is_long: true,
            taker: "Buyer".to_string(),
            price: Decimal::new(220_976, 2),
            size: Decimal::new(1, 2),
            leverage: Decimal::TEN,
            created_at: "2026-02-06T14:52:25.613Z".to_string(),
            filled_direction: "Long".to_string(),
            fee: Decimal::new(2, 2),
            fee_asset: String::new(),
        };
        let records = [record];
        let event = DeepXFinalizedPerpTrade {
            block_hash: [1; 32],
            block_number: 42,
            event_index: 1,
            market_id: 3,
            buyer_order_id: 87_561,
            buyer: account,
            seller_order_id: 1,
            seller: [2; 20],
            price: 2_209_760_000,
            size: 1,
            buyer_leverage: 10_000,
            seller_leverage: 1_000,
            filled_direction: "Long".to_string(),
            taker: "Buyer".to_string(),
            taker_fee: -20_000,
            maker_fee: 0,
            fee_asset: b"USDC".to_vec(),
        };
        let mut trades = [event];
        let scope = DeepXWsPerpFinalizedTradeScope {
            subaccount: account,
            base_decimal: 2,
            quote_decimal: 6,
            quote_symbol: "USDC",
        };
        let matched =
            match_perp_trade_history_batch_to_finalized(&batch, &request, &records, &trades, scope)
                .unwrap();
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].0, &records[0]);
        assert_eq!(matched[0].1, &trades[0]);

        let foreign = DeepXWsPerpFinalizedTradeScope {
            subaccount: [3; 20],
            ..scope
        };
        assert!(matches!(
            match_perp_trade_history_batch_to_finalized(
                &batch, &request, &records, &trades, foreign,
            ),
            Err(DeepXWsPerpTradeHistoryMatchError::FinalizedAccountScope)
        ));
        trades[0].price += 1;
        assert!(matches!(
            match_perp_trade_history_batch_to_finalized(&batch, &request, &records, &trades, scope,),
            Err(DeepXWsPerpTradeHistoryMatchError::Finalized(
                DeepXPerpTradeMatchError::NotFound
            ))
        ));
    }

    #[rstest]
    fn trade_batch_binds_account_and_preserves_numeric_lexemes() {
        let request = DeepXWsPerpTradeHistoryRequest::new(3, ADDRESS).unwrap();
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsPerpTradeHistoryFrame::parse(DATA, &request).unwrap()
        else {
            panic!("expected trade batch");
        };

        assert_eq!(batch.subaccount(), ADDRESS);
        assert_eq!(batch.items().len(), 1);
        assert!(batch.items()[0].raw().get().contains("2209.7600"));
        assert_eq!(batch.timestamp_ms(), 1_791_475_653_766);
    }

    #[rstest]
    fn trade_batch_rejects_foreign_account_and_continuation() {
        let request = DeepXWsPerpTradeHistoryRequest::new(3, ADDRESS).unwrap();
        let foreign = DATA.replace(ADDRESS, "0x977654069311fa41b88f7901dc65fae9a76fd60b");
        assert!(DeepXWsPerpTradeHistoryFrame::parse(&foreign, &request).is_err());

        let continued = DATA.replace("\"hasNext\":false", "\"hasNext\":true");
        assert!(DeepXWsPerpTradeHistoryFrame::parse(&continued, &request).is_err());
    }
}
