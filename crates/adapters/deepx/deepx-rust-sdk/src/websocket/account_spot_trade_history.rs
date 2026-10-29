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

//! Spot account trade-history subscription protocol.

use std::{borrow::Borrow, collections::HashSet};

use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use thiserror::Error;

use super::{account::DeepXWsUserBalancesRequest, spot_trades::DeepXWsSpotMarket};
use crate::{
    parse::parse_canonical_order_id,
    query::DeepXSpotAccountTradesRequest,
    rest::{
        DeepXAccountPage,
        models::{DeepXSpotAccountTradeRecord, DeepXSpotMarket},
    },
    rpc::{
        events::DeepXFinalizedSpotTrade,
        trade_match::{DeepXSpotTradeMatchError, match_finalized_spot_account_trade},
    },
};

/// Failures matching a Spot WebSocket batch to REST account trades.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeepXWsSpotTradeHistoryRestMatchError {
    /// The REST request did not select the exact subscribed account and pair.
    #[error("DeepX Spot trade REST request conflicts with WebSocket scope")]
    RequestScope,
    /// An item timestamp cannot be compared with the REST request bounds.
    #[error("invalid DeepX Spot trade timestamp")]
    InvalidTimestamp,
    /// A WebSocket item has no matching REST record.
    #[error("DeepX Spot trade has no matching REST account record")]
    NotFound,
    /// More than one REST record matches the same WebSocket item.
    #[error("DeepX Spot trade matches multiple REST account records")]
    Ambiguous,
    /// Separate WebSocket items consume the same REST trade identity.
    #[error("DeepX Spot trade batch reuses one REST trade identity")]
    DuplicateTrade,
}

/// Failures matching one Spot WebSocket batch through REST to finalized events.
#[derive(Debug, Error)]
pub enum DeepXWsSpotTradeHistoryMatchError {
    /// The REST request did not select the exact subscribed account and pair.
    #[error("DeepX Spot trade REST request conflicts with WebSocket scope")]
    RequestScope,
    /// An item timestamp cannot be compared with the REST request bounds.
    #[error("invalid DeepX Spot trade timestamp")]
    InvalidTimestamp,
    /// A WebSocket item has no matching REST record.
    #[error("DeepX Spot trade has no matching REST account record")]
    NotFound,
    /// More than one REST record matches the same WebSocket item.
    #[error("DeepX Spot trade matches multiple REST account records")]
    Ambiguous,
    /// Separate WebSocket items consume the same REST trade identity.
    #[error("DeepX Spot trade batch reuses one REST trade identity")]
    DuplicateTrade,
    /// The WebSocket subscription does not bind the supplied chain account.
    #[error("DeepX Spot trade subaccount conflicts with finalized account")]
    FinalizedAccountScope,
    /// One REST record could not be matched to finalized event evidence.
    #[error(transparent)]
    Finalized(#[from] DeepXSpotTradeMatchError),
}

impl From<DeepXWsSpotTradeHistoryRestMatchError> for DeepXWsSpotTradeHistoryMatchError {
    fn from(value: DeepXWsSpotTradeHistoryRestMatchError) -> Self {
        match value {
            DeepXWsSpotTradeHistoryRestMatchError::RequestScope => Self::RequestScope,
            DeepXWsSpotTradeHistoryRestMatchError::InvalidTimestamp => Self::InvalidTimestamp,
            DeepXWsSpotTradeHistoryRestMatchError::NotFound => Self::NotFound,
            DeepXWsSpotTradeHistoryRestMatchError::Ambiguous => Self::Ambiguous,
            DeepXWsSpotTradeHistoryRestMatchError::DuplicateTrade => Self::DuplicateTrade,
        }
    }
}

/// Independently verified account and market units for Spot chain matching.
#[derive(Clone, Copy, Debug)]
pub struct DeepXWsSpotFinalizedTradeScope<'a> {
    /// AccountId20 bound to the WebSocket subscription and REST request.
    pub subaccount: [u8; 20],
    /// Base and quote asset decimal places from verified market metadata.
    pub base_decimal: u8,
    pub quote_decimal: u8,
    /// Base and quote asset symbols used to bind side-specific fees.
    pub base_symbol: &'a str,
    pub quote_symbol: &'a str,
}

/// One Spot market and subaccount selected from the current market directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsSpotTradeHistoryRequest {
    market_name: String,
    pair: String,
    subaccount: String,
}

impl DeepXWsSpotTradeHistoryRequest {
    /// Creates an exact market- and subaccount-bound read-only subscription.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed market identity or AccountId20.
    pub fn new(market: &DeepXSpotMarket, subaccount: &str) -> Result<Self> {
        ensure!(
            !market.name.trim().is_empty(),
            "DeepX Spot market name is empty"
        );
        ensure!(
            market.pair.len() == 66
                && market.pair.starts_with("0x")
                && hex::decode(&market.pair[2..]).is_ok(),
            "DeepX Spot market pair is invalid"
        );
        Ok(Self {
            market_name: market.name.clone(),
            pair: market.pair.to_ascii_lowercase(),
            subaccount: DeepXWsUserBalancesRequest::new(subaccount)?
                .subaccount()
                .to_string(),
        })
    }

    /// Returns the selected market name.
    #[must_use]
    pub fn market_name(&self) -> &str {
        &self.market_name
    }

    /// Returns the selected pair identity.
    #[must_use]
    pub fn pair(&self) -> &str {
        &self.pair
    }

    /// Returns the selected subaccount.
    #[must_use]
    pub fn subaccount(&self) -> &str {
        &self.subaccount
    }

    /// Serializes the documented uncompressed subscription.
    #[must_use]
    pub fn to_text(&self) -> String {
        json!({
            "action": "subscribe",
            "market": {"type": "spot", "name": self.market_name},
            "subscriptions": [{
                "channel": "user_spot_trade_history",
                "address": self.subaccount,
            }],
            "options": {"compress": false},
        })
        .to_string()
    }

    /// Validates the exact market identity echoed by a frame.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign market name or pair.
    pub fn validate_market(&self, market: &DeepXWsSpotMarket) -> Result<()> {
        ensure!(
            market.kind == "spot"
                && market.name == self.market_name
                && market
                    .pair
                    .as_ref()
                    .is_none_or(|pair| pair.eq_ignore_ascii_case(&self.pair)),
            "foreign DeepX Spot trade-history market"
        );
        Ok(())
    }
}

/// Parses and validates one Spot trade item for the subscribed market.
///
/// # Errors
///
/// Returns an error for malformed venue fields, an invalid timestamp, or a foreign market.
pub fn parse_spot_trade_history_item(
    raw: &RawValue,
    request: &DeepXWsSpotTradeHistoryRequest,
) -> Result<DeepXSpotAccountTradeRecord> {
    let record: DeepXSpotAccountTradeRecord = serde_json::from_str(raw.get())?;
    ensure!(
        parse_canonical_order_id(&record.order_id).is_some(),
        "invalid DeepX Spot trade order ID"
    );
    let created_at = record.created_at.parse::<jiff::Timestamp>()?;
    ensure!(
        record.id != 0
            && record.pair.eq_ignore_ascii_case(request.pair())
            && record.pair_name == request.market_name()
            && matches!(record.order_side.as_str(), "Buy" | "Sell")
            && matches!(record.taker.as_str(), "Buyer" | "Seller")
            && record.price > Decimal::ZERO
            && record.base_amount > Decimal::ZERO
            && record.quote_amount > Decimal::ZERO
            && created_at.as_nanosecond() >= 0,
        "foreign or invalid DeepX Spot trade-history item"
    );
    Ok(record)
}

/// One unconfirmed Spot trade item with its original JSON.
#[derive(Debug)]
pub struct DeepXWsSpotTradeHistoryItem<T = DeepXSpotAccountTradeRecord> {
    record: T,
    raw: Box<RawValue>,
}

impl<T> DeepXWsSpotTradeHistoryItem<T> {
    /// Returns the exact venue record.
    #[must_use]
    pub const fn record(&self) -> &T {
        &self.record
    }

    /// Returns the original JSON with numeric lexemes preserved.
    #[must_use]
    pub fn raw(&self) -> &RawValue {
        &self.raw
    }

    /// Consumes the item into its record and original JSON.
    #[must_use]
    pub fn into_parts(self) -> (T, Box<RawValue>) {
        (self.record, self.raw)
    }

    /// Replaces the decoded record while preserving its original JSON.
    #[must_use]
    pub fn map_record<U>(self, map: impl FnOnce(T) -> U) -> DeepXWsSpotTradeHistoryItem<U> {
        DeepXWsSpotTradeHistoryItem {
            record: map(self.record),
            raw: self.raw,
        }
    }
}

/// One incremental Spot trade batch for the selected account and market.
#[derive(Debug)]
pub struct DeepXWsSpotTradeHistoryBatch<T = DeepXSpotAccountTradeRecord> {
    items: Vec<DeepXWsSpotTradeHistoryItem<T>>,
    timestamp_ms: u64,
}

impl<T> DeepXWsSpotTradeHistoryBatch<T> {
    /// Returns trade observations in venue order.
    #[must_use]
    pub fn items(&self) -> &[DeepXWsSpotTradeHistoryItem<T>] {
        &self.items
    }

    /// Returns the venue frame timestamp in Unix milliseconds.
    #[must_use]
    pub const fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    /// Consumes the batch into its items and timestamp.
    #[must_use]
    pub fn into_parts(self) -> (Vec<DeepXWsSpotTradeHistoryItem<T>>, u64) {
        (self.items, self.timestamp_ms)
    }

    /// Replaces each decoded record while preserving the original JSON and venue order.
    #[must_use]
    pub fn map_records<U>(self, mut map: impl FnMut(T) -> U) -> DeepXWsSpotTradeHistoryBatch<U> {
        DeepXWsSpotTradeHistoryBatch {
            items: self
                .items
                .into_iter()
                .map(|item| item.map_record(&mut map))
                .collect(),
            timestamp_ms: self.timestamp_ms,
        }
    }
}

/// Matches every incremental Spot trade to one validated REST account record atomically.
///
/// The supplied records are mutable candidate evidence. A match does not establish chain finality,
/// stream continuity, or permission to dispatch a fill.
///
/// # Errors
///
/// Returns an error for scope conflict, invalid timestamps, missing or ambiguous records, or a
/// duplicate matched trade identity.
pub fn match_spot_trade_history_batch_to_rest<'a, W, R>(
    batch: &DeepXWsSpotTradeHistoryBatch<W>,
    subscription: &DeepXWsSpotTradeHistoryRequest,
    request: &DeepXSpotAccountTradesRequest,
    records: &'a [R],
) -> Result<Vec<&'a R>, DeepXWsSpotTradeHistoryRestMatchError>
where
    W: Borrow<DeepXSpotAccountTradeRecord>,
    R: Borrow<DeepXSpotAccountTradeRecord>,
{
    use DeepXWsSpotTradeHistoryRestMatchError as Error;

    if request.validate().is_err()
        || !request
            .subaccount
            .eq_ignore_ascii_case(subscription.subaccount())
        || request.name.is_some()
        || !request
            .pair
            .as_ref()
            .is_some_and(|pair| pair.eq_ignore_ascii_case(subscription.pair()))
        || request.order_id.is_some()
        || request.order_side.is_some()
        || request.cursor.is_some()
    {
        return Err(Error::RequestScope);
    }
    let mut matched = Vec::with_capacity(batch.items().len());
    let mut trade_ids = HashSet::with_capacity(batch.items().len());

    for item in batch.items() {
        let item_record: &DeepXSpotAccountTradeRecord = item.record().borrow();
        let timestamp = item_record
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
            let record: &DeepXSpotAccountTradeRecord = (*candidate).borrow();
            record.id == item_record.id
                && record.order_id == item_record.order_id
                && record.order_side == item_record.order_side
                && record.pair.eq_ignore_ascii_case(subscription.pair())
                && record.pair_name == subscription.market_name()
                && record.base_amount == item_record.base_amount
                && record.quote_amount == item_record.quote_amount
                && record.price == item_record.price
                && record.fee == item_record.fee
                && record
                    .fee_asset
                    .eq_ignore_ascii_case(&item_record.fee_asset)
                && record.taker == item_record.taker
                && record
                    .created_at
                    .parse::<jiff::Timestamp>()
                    .is_ok_and(|value| value == timestamp)
        });
        let record = candidates.next().ok_or(Error::NotFound)?;
        if candidates.next().is_some() {
            return Err(Error::Ambiguous);
        }
        if !trade_ids.insert(<R as Borrow<DeepXSpotAccountTradeRecord>>::borrow(record).id) {
            return Err(Error::DuplicateTrade);
        }
        matched.push(record);
    }
    Ok(matched)
}

/// Matches every Spot WebSocket item through REST to supplied finalized chain events.
///
/// The caller independently validates REST pages, market metadata, and canonical finality. This
/// pure match does not prove stream continuity or historical REST completeness.
///
/// # Errors
///
/// Returns an error if account scope conflicts or either exact match stage fails.
pub fn match_spot_trade_history_batch_to_finalized<'a, 'b, W, R>(
    batch: &DeepXWsSpotTradeHistoryBatch<W>,
    subscription: &DeepXWsSpotTradeHistoryRequest,
    request: &DeepXSpotAccountTradesRequest,
    records: &'a [R],
    trades: &'b [DeepXFinalizedSpotTrade],
    scope: DeepXWsSpotFinalizedTradeScope<'_>,
) -> Result<Vec<(&'a R, &'b DeepXFinalizedSpotTrade)>, DeepXWsSpotTradeHistoryMatchError>
where
    W: Borrow<DeepXSpotAccountTradeRecord>,
    R: Borrow<DeepXSpotAccountTradeRecord>,
{
    use DeepXWsSpotTradeHistoryMatchError as Error;

    let encoded_subaccount = format!("0x{}", hex::encode(scope.subaccount));
    if !subscription
        .subaccount()
        .eq_ignore_ascii_case(&encoded_subaccount)
    {
        return Err(Error::FinalizedAccountScope);
    }
    let records = match_spot_trade_history_batch_to_rest(batch, subscription, request, records)?;
    records
        .into_iter()
        .map(|record| {
            let trade = match_finalized_spot_account_trade(
                trades,
                record.borrow(),
                scope.subaccount,
                scope.base_decimal,
                scope.quote_decimal,
                scope.base_symbol,
                scope.quote_symbol,
            )?;
            Ok((record, trade))
        })
        .collect()
}

/// One frame admitted for an exact Spot trade-history subscription.
#[derive(Debug)]
pub enum DeepXWsMappedSpotTradeHistoryFrame<T> {
    /// The selected market and subaccount were acknowledged.
    Subscribed,
    /// One mutable incremental trade batch.
    TradeBatch(DeepXWsSpotTradeHistoryBatch<T>),
    /// Application heartbeat response.
    Pong,
}

/// A Spot trade-history frame containing SDK-owned records.
pub type DeepXWsSpotTradeHistoryFrame =
    DeepXWsMappedSpotTradeHistoryFrame<DeepXSpotAccountTradeRecord>;

impl<T: From<DeepXSpotAccountTradeRecord>> DeepXWsMappedSpotTradeHistoryFrame<T> {
    /// Parses one frame under the selected market and subaccount identity.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, foreign, or unsupported frames and invalid trade records.
    pub fn parse(text: &str, request: &DeepXWsSpotTradeHistoryRequest) -> Result<Self> {
        #[derive(Deserialize)]
        struct WireFrame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsSpotMarket>,
            subscriptions: Option<Vec<Subscription>>,
            channel: Option<String>,
            data: Option<Box<RawValue>>,
            timestamp: Option<u64>,
        }
        #[derive(Deserialize)]
        struct Subscription {
            channel: String,
            address: String,
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
                request.validate_market(
                    &wire
                        .market
                        .context("missing DeepX Spot trade-history market")?,
                )?;
                let subscriptions = wire
                    .subscriptions
                    .context("missing DeepX Spot trade-history subscriptions")?;
                ensure!(
                    subscriptions.len() == 1
                        && subscriptions[0].channel == "user_spot_trade_history"
                        && subscriptions[0]
                            .address
                            .eq_ignore_ascii_case(request.subaccount()),
                    "foreign DeepX Spot trade-history acknowledgement"
                );
                Ok(Self::Subscribed)
            }
            "data" => {
                request.validate_market(
                    &wire
                        .market
                        .context("missing DeepX Spot trade-history market")?,
                )?;
                ensure!(
                    wire.channel.as_deref() == Some("user_spot_trade_history"),
                    "foreign DeepX Spot trade-history channel"
                );
                let data: Data = serde_json::from_str(
                    wire.data
                        .context("missing DeepX Spot trade-history data")?
                        .get(),
                )?;
                ensure!(
                    data.address.eq_ignore_ascii_case(request.subaccount())
                        && !data.trades.has_next
                        && data.trades.next_cursor.is_none(),
                    "foreign or incomplete DeepX Spot trade-history batch"
                );
                let mut items = Vec::with_capacity(data.trades.items.len());
                for raw in data.trades.items {
                    let record = parse_spot_trade_history_item(&raw, request)?.into();
                    items.push(DeepXWsSpotTradeHistoryItem { record, raw });
                }
                let timestamp_ms = wire
                    .timestamp
                    .context("missing DeepX Spot trade-history timestamp")?;
                ensure!(
                    timestamp_ms > 0 && i64::try_from(timestamp_ms).is_ok(),
                    "invalid DeepX Spot trade-history timestamp"
                );
                Ok(Self::TradeBatch(DeepXWsSpotTradeHistoryBatch {
                    items,
                    timestamp_ms,
                }))
            }
            _ => anyhow::bail!("unsupported DeepX Spot trade-history frame"),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{query::DeepXAccountSortOrder, rest::DeepXApiResponse};

    const ADDRESS: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
    const PAIR: &str = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
    const DATA: &str = r#"{"type":"data","channel":"user_spot_trade_history","market":{"type":"spot","name":"ETH/USDC","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","trades":{"items":[{"id":53219022000005,"orderId":"2524638","orderSide":"Sell","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37","pairName":"ETH/USDC","baseAmount":0.0011644,"quoteAmount":2,"price":1717.64,"fee":0.0002,"taker":"Buyer","createdAt":"2026-06-18T15:13:14.739Z"}],"nextCursor":null,"hasNext":false}},"timestamp":1781795594739}"#;

    fn request() -> DeepXWsSpotTradeHistoryRequest {
        let response: DeepXApiResponse<Vec<DeepXSpotMarket>> = serde_json::from_str(include_str!(
            "../../test_data/http/testnet/spot_markets.json"
        ))
        .unwrap();
        let market = response
            .data
            .iter()
            .find(|market| market.name == "ETH/USDC")
            .unwrap();
        assert_eq!(market.pair, PAIR);
        DeepXWsSpotTradeHistoryRequest::new(market, ADDRESS).unwrap()
    }

    fn rest_request() -> DeepXSpotAccountTradesRequest {
        DeepXSpotAccountTradesRequest {
            subaccount: ADDRESS.to_string(),
            order_id: None,
            order_side: None,
            name: None,
            pair: Some(PAIR.to_string()),
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: Some(1_781_795_594_739),
            end_ms: Some(1_781_795_594_739),
            page_size: Some(100),
        }
    }

    fn batch() -> DeepXWsSpotTradeHistoryBatch {
        let DeepXWsSpotTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsSpotTradeHistoryFrame::parse(DATA, &request()).unwrap()
        else {
            panic!("expected Spot trade batch");
        };
        batch
    }

    #[rstest]
    fn mapped_frame_preserves_sdk_record_and_original_json() {
        struct Wrapped(DeepXSpotAccountTradeRecord);

        impl From<DeepXSpotAccountTradeRecord> for Wrapped {
            fn from(record: DeepXSpotAccountTradeRecord) -> Self {
                Self(record)
            }
        }

        let DeepXWsMappedSpotTradeHistoryFrame::TradeBatch(batch):
            DeepXWsMappedSpotTradeHistoryFrame<Wrapped> =
            DeepXWsMappedSpotTradeHistoryFrame::parse(DATA, &request()).unwrap()
        else {
            panic!("expected Spot trade batch");
        };
        assert_eq!(batch.items()[0].record().0.id, 53_219_022_000_005);
        assert!(batch.items()[0].raw().get().contains("0.0011644"));
    }

    #[rstest]
    fn spot_trade_batch_requires_matching_finalized_event_and_account_scope() {
        let batch = batch();
        let record = batch.items()[0].record().clone();
        let mut base_amount = [0_u8; 32];
        base_amount[..16].copy_from_slice(&1_164_400_000_000_000_u128.to_le_bytes());
        let mut quote_amount = [0_u8; 32];
        quote_amount[..16].copy_from_slice(&2_000_000_u128.to_le_bytes());
        let mut price = [0_u8; 32];
        price[..16].copy_from_slice(&1_717_640_000_u128.to_le_bytes());
        let event = DeepXFinalizedSpotTrade {
            block_hash: [1; 32],
            block_number: 53_219_022,
            event_index: 5,
            pair: hex::decode(PAIR.trim_start_matches("0x"))
                .unwrap()
                .try_into()
                .unwrap(),
            buyer: [0x11; 20],
            buy_id: 9,
            seller: hex::decode(ADDRESS.trim_start_matches("0x"))
                .unwrap()
                .try_into()
                .unwrap(),
            sell_id: 2_524_638,
            trade_amount_u: quote_amount,
            trade_amount_b: base_amount,
            taker: "Buyer".to_string(),
            taker_fee: 100_000_000_000_000,
            taker_fee_asset: b"ETH".to_vec(),
            maker_fee: -200,
            maker_fee_asset: b"USDC".to_vec(),
            price,
        };
        let scope = DeepXWsSpotFinalizedTradeScope {
            subaccount: event.seller,
            base_decimal: 18,
            quote_decimal: 6,
            base_symbol: "ETH",
            quote_symbol: "USDC",
        };
        let events = [event.clone()];
        let records = [record];
        let matched = match_spot_trade_history_batch_to_finalized(
            &batch,
            &request(),
            &rest_request(),
            &records,
            &events,
            scope,
        )
        .unwrap();
        assert_eq!(matched[0].1.event_index, 5);
        assert!(matches!(
            match_spot_trade_history_batch_to_finalized(
                &batch,
                &request(),
                &rest_request(),
                &records,
                &[],
                scope,
            ),
            Err(DeepXWsSpotTradeHistoryMatchError::Finalized(
                DeepXSpotTradeMatchError::NotFound
            ))
        ));
        let mut foreign = event;
        foreign.seller = [0x22; 20];
        assert!(matches!(
            match_spot_trade_history_batch_to_finalized(
                &batch,
                &request(),
                &rest_request(),
                &records,
                &[foreign],
                scope,
            ),
            Err(DeepXWsSpotTradeHistoryMatchError::Finalized(
                DeepXSpotTradeMatchError::NotFound
            ))
        ));
        let wrong_scope = DeepXWsSpotFinalizedTradeScope {
            subaccount: [0x22; 20],
            ..scope
        };
        assert!(matches!(
            match_spot_trade_history_batch_to_finalized(
                &batch,
                &request(),
                &rest_request(),
                &records,
                &events,
                wrong_scope,
            ),
            Err(DeepXWsSpotTradeHistoryMatchError::FinalizedAccountScope)
        ));
    }

    #[rstest]
    fn subscription_ack_binds_market_and_address() {
        let ack = json!({
            "type": "subscribed",
            "market": {"type": "spot", "name": "ETH/USDC"},
            "subscriptions": [{"channel": "user_spot_trade_history", "address": ADDRESS}],
        });
        assert!(matches!(
            DeepXWsSpotTradeHistoryFrame::parse(&ack.to_string(), &request()).unwrap(),
            DeepXWsSpotTradeHistoryFrame::Subscribed
        ));
        let mut foreign = ack;
        foreign["subscriptions"][0]["address"] =
            json!("0x977654069311fa41b88f7901dc65fae9a76fd60b");
        assert!(DeepXWsSpotTradeHistoryFrame::parse(&foreign.to_string(), &request()).is_err());
    }

    #[rstest]
    fn spot_trade_batch_preserves_exact_record_and_rejects_foreign_scope() {
        let frame: serde_json::Value = serde_json::from_str(DATA).unwrap();
        let DeepXWsSpotTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsSpotTradeHistoryFrame::parse(&frame.to_string(), &request()).unwrap()
        else {
            panic!("expected Spot trade batch");
        };
        assert_eq!(batch.items().len(), 1);
        assert_eq!(batch.items()[0].record().id, 53219022000005);
        assert_eq!(
            batch.items()[0].record().base_amount,
            "0.0011644".parse::<Decimal>().unwrap()
        );
        assert!(batch.items()[0].raw().get().contains("baseAmount"));
        let mut first_generated = frame.clone();
        first_generated["data"]["trades"]["items"][0]["orderId"] = json!("0");
        let DeepXWsSpotTradeHistoryFrame::TradeBatch(first_generated) =
            DeepXWsSpotTradeHistoryFrame::parse(&first_generated.to_string(), &request()).unwrap()
        else {
            panic!("expected zero-ID Spot trade batch");
        };
        assert_eq!(first_generated.items()[0].record().order_id, "0");

        for (field, value) in [
            (
                "address",
                json!("0x977654069311fa41b88f7901dc65fae9a76fd60b"),
            ),
            (
                "pair",
                json!("0x0000000000000000000000000000000000000000000000000000000000000000"),
            ),
        ] {
            let mut invalid = frame.clone();
            if field == "address" {
                invalid["data"]["address"] = value;
            } else {
                invalid["data"]["trades"]["items"][0][field] = value;
            }
            assert!(DeepXWsSpotTradeHistoryFrame::parse(&invalid.to_string(), &request()).is_err());
        }
    }

    #[rstest]
    fn incomplete_or_invalid_spot_trade_batch_fails_closed() {
        let mut continued: serde_json::Value = serde_json::from_str(DATA).unwrap();
        continued["data"]["trades"]["hasNext"] = json!(true);
        continued["data"]["trades"]["nextCursor"] = json!("next");
        assert!(DeepXWsSpotTradeHistoryFrame::parse(&continued.to_string(), &request()).is_err());

        let mut invalid: serde_json::Value = serde_json::from_str(DATA).unwrap();
        invalid["data"]["trades"]["items"][0]["baseAmount"] = json!(0);
        assert!(DeepXWsSpotTradeHistoryFrame::parse(&invalid.to_string(), &request()).is_err());
        invalid["data"]["trades"]["items"][0]["baseAmount"] = json!("0.0011644");
        invalid["data"]["trades"]["items"][0]["id"] = json!(0);
        assert!(DeepXWsSpotTradeHistoryFrame::parse(&invalid.to_string(), &request()).is_err());
    }

    #[rstest]
    fn rest_match_rejects_scope_and_value_conflicts() {
        let batch = batch();
        let record = batch.items()[0].record().clone();
        let rest_request = rest_request();
        let records = [record.clone()];
        let matched =
            match_spot_trade_history_batch_to_rest(&batch, &request(), &rest_request, &records)
                .unwrap();
        assert!(std::ptr::eq(matched[0], records.as_ptr()));

        let mut foreign_request = rest_request.clone();
        foreign_request.subaccount = "0x977654069311fa41b88f7901dc65fae9a76fd60b".into();
        assert!(matches!(
            match_spot_trade_history_batch_to_rest(&batch, &request(), &foreign_request, &records),
            Err(DeepXWsSpotTradeHistoryRestMatchError::RequestScope),
        ));
        foreign_request = rest_request.clone();
        foreign_request.start_ms = Some(1_781_795_594_740);
        foreign_request.end_ms = None;
        assert!(matches!(
            match_spot_trade_history_batch_to_rest(&batch, &request(), &foreign_request, &records),
            Err(DeepXWsSpotTradeHistoryRestMatchError::RequestScope),
        ));
        let mut different = record;
        different.fee += Decimal::ONE;
        assert!(matches!(
            match_spot_trade_history_batch_to_rest(&batch, &request(), &rest_request, &[different]),
            Err(DeepXWsSpotTradeHistoryRestMatchError::NotFound),
        ));
    }

    #[rstest]
    fn rest_match_rejects_ambiguity_and_reused_identity() {
        let batch = batch();
        let record = batch.items()[0].record().clone();
        assert!(matches!(
            match_spot_trade_history_batch_to_rest(
                &batch,
                &request(),
                &rest_request(),
                &[record.clone(), record.clone()],
            ),
            Err(DeepXWsSpotTradeHistoryRestMatchError::Ambiguous),
        ));
        let mut duplicate_frame: serde_json::Value = serde_json::from_str(DATA).unwrap();
        let item = duplicate_frame["data"]["trades"]["items"][0].clone();
        duplicate_frame["data"]["trades"]["items"] = json!([item.clone(), item]);
        let DeepXWsSpotTradeHistoryFrame::TradeBatch(duplicate_batch) =
            DeepXWsSpotTradeHistoryFrame::parse(&duplicate_frame.to_string(), &request()).unwrap()
        else {
            panic!("expected duplicate Spot trade batch");
        };
        assert!(matches!(
            match_spot_trade_history_batch_to_rest(
                &duplicate_batch,
                &request(),
                &rest_request(),
                &[record],
            ),
            Err(DeepXWsSpotTradeHistoryRestMatchError::DuplicateTrade),
        ));
    }

    #[rstest]
    fn spot_batch_preserves_exact_item_and_scope() {
        let DeepXWsSpotTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsSpotTradeHistoryFrame::parse(DATA, &request()).unwrap()
        else {
            panic!("expected Spot trade batch");
        };
        assert_eq!(batch.items().len(), 1);
        assert_eq!(batch.items()[0].record().id, 53_219_022_000_005);
        assert!(batch.items()[0].raw().get().contains("0.0011644"));
        assert_eq!(batch.timestamp_ms(), 1_781_795_594_739);
    }

    #[rstest]
    fn spot_batch_rejects_foreign_account_and_continuation() {
        let foreign = DATA.replace(ADDRESS, "0x977654069311fa41b88f7901dc65fae9a76fd60b");
        assert!(DeepXWsSpotTradeHistoryFrame::parse(&foreign, &request()).is_err());
        let continued = DATA.replace("\"hasNext\":false", "\"hasNext\":true");
        assert!(DeepXWsSpotTradeHistoryFrame::parse(&continued, &request()).is_err());
    }
}
