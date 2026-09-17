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

//! Exact-decimal, sequence-bound public perpetual book reconstruction.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
};

use rust_decimal::Decimal;
use serde::Deserialize;

use super::public::{DeepXWsPublicChannel, DeepXWsPublicFrame};
use crate::http::models::exact_decimal;

/// A price level in the server's configured, potentially aggregated book.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DeepXWsBookLevel {
    /// Exact server price.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub price: Decimal,
    /// Exact quantity; zero deletes the level in a delta.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub qty: Decimal,
    /// Server-provided notional, not a locally inferred quantity.
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    pub value: Decimal,
}

/// A validated local book, with both sides ordered by ascending price.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXWsBookSnapshot {
    /// Last sequence identifier accepted from the server.
    pub last_update_id: u64,
    /// Server engine time in milliseconds.
    pub engine_time: u64,
    /// Bid levels; the final entry is the best bid.
    pub bids: BTreeMap<Decimal, DeepXWsBookLevel>,
    /// Ask levels; the first entry is the best ask.
    pub asks: BTreeMap<Decimal, DeepXWsBookLevel>,
}

/// Book failures requiring a fresh server snapshot before further deltas.
#[derive(Debug, thiserror::Error)]
pub enum DeepXWsBookError {
    /// The envelope was not the expected perpetual book channel.
    #[error("DeepX book frame scope mismatch")]
    Scope,
    /// The payload could not be decoded exactly.
    #[error("invalid DeepX book payload: {0}")]
    Decode(#[from] serde_json::Error),
    /// A delta arrived without a snapshot or with the wrong predecessor.
    #[error("DeepX book requires a fresh snapshot after a sequence gap")]
    SequenceGap,
    /// Levels violated numeric or unique-price requirements.
    #[error("invalid or duplicate DeepX book level")]
    InvalidLevel,
    /// The configured retained-level limit was exceeded.
    #[error("DeepX book exceeds retained level capacity")]
    Capacity,
}

/// A bounded market-owned book; any failed update invalidates the cached book.
///
/// Sequence IDs are opaque: continuity uses `prevLastUpdateId`, not an assumed increment of one.
/// This is the server's depth/price-aggregation view, not a claim of full exchange depth.
#[derive(Debug)]
pub struct DeepXWsBookStream {
    market_id: u16,
    capacity: NonZeroUsize,
    snapshot: Option<DeepXWsBookSnapshot>,
}

impl DeepXWsBookStream {
    /// Creates a stream bounded by total retained levels across both sides.
    #[must_use]
    pub const fn new(market_id: u16, capacity: NonZeroUsize) -> Self {
        Self {
            market_id,
            capacity,
            snapshot: None,
        }
    }

    /// Returns a valid book, or none until a fresh snapshot has been accepted.
    #[must_use]
    pub const fn snapshot(&self) -> Option<&DeepXWsBookSnapshot> {
        self.snapshot.as_ref()
    }

    /// Applies a complete update atomically, returning true for a snapshot, and invalidates on failure.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed/foreign data, sequence gaps, invalid levels or capacity excess.
    pub fn ingest(&mut self, frame: &DeepXWsPublicFrame) -> Result<bool, DeepXWsBookError> {
        let result = self.apply(frame);
        match result {
            Ok((snapshot, is_snapshot)) => {
                self.snapshot = Some(snapshot);
                Ok(is_snapshot)
            }
            Err(e) => {
                self.snapshot = None;
                Err(e)
            }
        }
    }

    fn apply(
        &self,
        frame: &DeepXWsPublicFrame,
    ) -> Result<(DeepXWsBookSnapshot, bool), DeepXWsBookError> {
        let DeepXWsPublicFrame::Data {
            market,
            channel: DeepXWsPublicChannel::Orderbook,
            data,
            ..
        } = frame
        else {
            return Err(DeepXWsBookError::Scope);
        };
        if market.kind != "perp" || market.id != self.market_id {
            return Err(DeepXWsBookError::Scope);
        }
        let update: BookUpdate = serde_json::from_str(data.get())?;
        if update.bids.len().saturating_add(update.asks.len()) > self.capacity.get() {
            return Err(DeepXWsBookError::Capacity);
        }
        let is_snapshot = matches!(update.update_type, UpdateType::Snapshot);
        let mut next = match update.update_type {
            UpdateType::Snapshot => DeepXWsBookSnapshot {
                last_update_id: update.last_update_id,
                engine_time: update.engine_time,
                bids: BTreeMap::new(),
                asks: BTreeMap::new(),
            },
            UpdateType::Delta => {
                let current = self
                    .snapshot
                    .as_ref()
                    .ok_or(DeepXWsBookError::SequenceGap)?;
                if update.prev_last_update_id != Some(current.last_update_id) {
                    return Err(DeepXWsBookError::SequenceGap);
                }
                current.clone()
            }
        };
        apply_levels(&mut next.bids, update.bids)?;
        apply_levels(&mut next.asks, update.asks)?;
        if next.bids.len().saturating_add(next.asks.len()) > self.capacity.get() {
            return Err(DeepXWsBookError::Capacity);
        }
        next.last_update_id = update.last_update_id;
        next.engine_time = update.engine_time;
        Ok((next, is_snapshot))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookUpdate {
    update_type: UpdateType,
    prev_last_update_id: Option<u64>,
    last_update_id: u64,
    engine_time: u64,
    bids: Vec<DeepXWsBookLevel>,
    asks: Vec<DeepXWsBookLevel>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum UpdateType {
    Snapshot,
    Delta,
}

fn apply_levels(
    side: &mut BTreeMap<Decimal, DeepXWsBookLevel>,
    levels: Vec<DeepXWsBookLevel>,
) -> Result<(), DeepXWsBookError> {
    let mut prices = BTreeSet::new();
    for level in levels {
        if level.price <= Decimal::ZERO
            || level.qty < Decimal::ZERO
            || level.value < Decimal::ZERO
            || !prices.insert(level.price)
        {
            return Err(DeepXWsBookError::InvalidLevel);
        }
        if level.qty.is_zero() {
            side.remove(&level.price);
        } else {
            side.insert(level.price, level);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    fn frame(
        kind: &str,
        previous: Option<u64>,
        id: u64,
        bids: serde_json::Value,
    ) -> DeepXWsPublicFrame {
        DeepXWsPublicFrame::parse(
            &json!({"type":"data", "channel":"orderbook",
            "market":{"type":"perp","id":2}, "timestamp":10,
            "data":{"updateType":kind,"prevLastUpdateId":previous,"lastUpdateId":id,
                "engineTime":9,"bids":bids,"asks":[]}})
            .to_string(),
        )
        .unwrap()
    }

    fn levels() -> serde_json::Value {
        json!([{"price":"78195.123456789012345678","qty":"0.0114","value":"891.4244074074074074059292"}])
    }

    #[rstest]
    fn snapshot_delta_delete_and_replacement() {
        let mut stream = DeepXWsBookStream::new(2, NonZeroUsize::new(4).unwrap());
        stream
            .ingest(&frame("snapshot", None, u64::MAX - 10, levels()))
            .unwrap();
        let price = stream
            .snapshot()
            .unwrap()
            .bids
            .first_key_value()
            .unwrap()
            .0
            .to_string();
        assert_eq!(price, "78195.123456789012345678");
        stream
            .ingest(&frame(
                "delta",
                Some(u64::MAX - 10),
                u64::MAX,
                json!([
            {"price":price,"qty":0,"value":0}, {"price":1,"qty":2,"value":2}]),
            ))
            .unwrap();
        assert_eq!(stream.snapshot().unwrap().bids.len(), 1);
        stream
            .ingest(&frame("snapshot", None, 2, json!([])))
            .unwrap();
        assert!(stream.snapshot().unwrap().bids.is_empty());
    }

    #[rstest]
    fn gaps_invalidate_and_only_snapshot_restores() {
        let mut stream = DeepXWsBookStream::new(2, NonZeroUsize::new(4).unwrap());
        assert!(matches!(
            stream.ingest(&frame("delta", Some(1), 2, levels())),
            Err(DeepXWsBookError::SequenceGap)
        ));
        stream
            .ingest(&frame("snapshot", None, 10, levels()))
            .unwrap();
        assert!(
            stream
                .ingest(&frame("delta", Some(9), 11, levels()))
                .is_err()
        );
        assert!(stream.snapshot().is_none());
        assert!(
            stream
                .ingest(&frame("delta", Some(10), 11, levels()))
                .is_err()
        );
        stream
            .ingest(&frame("snapshot", None, 12, levels()))
            .unwrap();
        assert_eq!(stream.snapshot().unwrap().last_update_id, 12);
    }

    #[rstest]
    #[case("duplicate")]
    #[case("negative")]
    #[case("capacity")]
    #[case("decode")]
    #[case("scope")]
    fn failed_updates_never_expose_partial_or_stale_books(#[case] mutation: &str) {
        let mut stream = DeepXWsBookStream::new(2, NonZeroUsize::new(2).unwrap());
        stream
            .ingest(&frame("snapshot", None, 1, levels()))
            .unwrap();
        let mut bids = json!([{"price":1,"qty":1,"value":1}]);
        match mutation {
            "duplicate" => {
                bids = json!([{"price":1,"qty":1,"value":1},{"price":"1.0","qty":2,"value":2}])
            }
            "negative" => bids[0]["qty"] = json!(-1),
            "capacity" => {
                bids = json!([{"price":1,"qty":1,"value":1},{"price":2,"qty":2,"value":4}])
            }
            "decode" => bids[0]["price"] = json!("not a decimal"),
            _ => {}
        }
        let mut update = frame("delta", Some(1), 2, bids);
        if mutation == "scope" {
            if let DeepXWsPublicFrame::Data { market, .. } = &mut update {
                market.id = 3;
            }
        }
        assert!(stream.ingest(&update).is_err());
        assert!(stream.snapshot().is_none());
    }
}
