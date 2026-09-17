// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Public trade-page initialization and bounded identity deduplication.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroUsize,
};

use rust_decimal::Decimal;

use super::public::{DeepXWsPublicChannel, DeepXWsPublicFrame};
use crate::http::{DeepXPerpTrade, DeepXPerpTradesPage};

/// A subscription-owned public trade stream; fresh connections require explicit reconciliation.
///
/// The first page seeds identities without emitting history as new live executions. Subsequent
/// pages emit unseen trades chronologically. Unknown trades older than the delivery watermark
/// require explicit recovery rather than silent filtering. No numeric trade-ID ordering is assumed.
#[derive(Clone, Debug)]
pub struct DeepXWsTradeStream {
    market_id: u16,
    capacity: NonZeroUsize,
    initialized: bool,
    watermark_ms: Option<i64>,
    recent: BTreeMap<u64, (i64, DeepXPerpTrade)>,
    order: VecDeque<u64>,
}

impl DeepXWsTradeStream {
    /// Creates an uninitialized market-bound stream with bounded retained identities.
    #[must_use]
    pub fn new(market_id: u16, capacity: NonZeroUsize) -> Self {
        Self {
            market_id,
            capacity,
            initialized: false,
            watermark_ms: None,
            recent: BTreeMap::new(),
            order: VecDeque::new(),
        }
    }

    /// Returns whether this subscription has accepted its initial history page.
    #[must_use]
    pub const fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Validates and atomically applies a public trade page, returning only new executions.
    ///
    /// Failed batches leave all initialization, identity and watermark state unchanged. Retained
    /// equal-watermark IDs are never evicted, preventing duplicate delivery at timestamp ties.
    ///
    /// # Errors
    ///
    /// Returns an error for foreign scope, malformed/unordered trades, conflicting IDs, unseen
    /// older executions or a page/timestamp tie exceeding the retained identity capacity.
    pub fn ingest(&mut self, frame: &DeepXWsPublicFrame) -> anyhow::Result<Vec<DeepXPerpTrade>> {
        let DeepXWsPublicFrame::Data {
            market,
            channel: DeepXWsPublicChannel::Trades,
            data,
            ..
        } = frame
        else {
            anyhow::bail!("DeepX trade stream requires a public trade data frame");
        };
        anyhow::ensure!(
            market.kind == "perp" && market.id == self.market_id,
            "DeepX trade stream market mismatch"
        );
        let page: DeepXPerpTradesPage = serde_json::from_str(data.get())?;
        anyhow::ensure!(
            page.items.len() <= self.capacity.get(),
            "DeepX trade page exceeds identity capacity"
        );
        self.apply_page(page.items)
    }

    /// Returns inclusive REST recovery bounds from the retained boundary and a fresh history page.
    ///
    /// The initial connection page must be fully validated. Its newest execution, rather than an
    /// inferred envelope timestamp or local clock, supplies the fixed upper bound. This does not
    /// prove backend historical completeness or stable pagination.
    ///
    /// # Errors
    ///
    /// Returns an error without mutation for missing boundaries, malformed/empty pages, or a fresh
    /// page whose newest execution precedes the retained boundary.
    pub fn recovery_window(&self, frame: &DeepXWsPublicFrame) -> anyhow::Result<(u64, u64)> {
        let start = self.watermark_ms.ok_or_else(|| {
            anyhow::anyhow!("DeepX trade recovery has no retained execution boundary")
        })?;
        let mut validation = Self::new(self.market_id, self.capacity);
        validation.ingest(frame)?;
        let end = validation
            .watermark_ms
            .ok_or_else(|| anyhow::anyhow!("DeepX fresh trade history page is empty"))?;
        anyhow::ensure!(
            end >= start,
            "DeepX fresh trade page precedes the recovery boundary"
        );
        Ok((u64::try_from(start)?, u64::try_from(end)?))
    }

    /// Reconciles a complete bounded REST history against a fresh connection history page.
    ///
    /// Every retained boundary identity must remain present and unchanged in REST. Fresh-page
    /// executions at or above that boundary must also be present and identical in REST; older
    /// fresh-page rows are validated but remain initialization history. Novel executions are
    /// returned chronologically, with the same bounded timestamp-tie retention as live pages.
    /// Failed reconciliation leaves all state unchanged. Callers must obtain complete REST pages,
    /// not a truncated record-limited response, and validate framework precision before publishing.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds/pages, missing/conflicting boundary or fresh-page IDs,
    /// malformed/unordered history, or exhausted retained identity capacity.
    pub fn reconcile_history(
        &mut self,
        history: &[DeepXPerpTrade],
        fresh: &DeepXWsPublicFrame,
    ) -> anyhow::Result<Vec<DeepXPerpTrade>> {
        let (start, end) = self.recovery_window(fresh)?;
        let by_id = history
            .iter()
            .map(|trade| (trade.id, trade))
            .collect::<BTreeMap<_, _>>();
        anyhow::ensure!(
            by_id.len() == history.len(),
            "duplicate DeepX recovery trade ID"
        );
        for trade in history {
            let timestamp: jiff::Timestamp = trade.created_at.parse()?;
            let time = timestamp.as_millisecond();
            anyhow::ensure!(
                time >= i64::try_from(start)? && time <= i64::try_from(end)?,
                "DeepX recovery trade is outside fixed bounds"
            );
        }
        for (id, (time, retained)) in &self.recent {
            if *time == i64::try_from(start)? {
                anyhow::ensure!(
                    by_id.get(id).is_some_and(|trade| **trade == *retained),
                    "DeepX REST recovery boundary trade is missing or conflicting"
                );
            }
        }
        let DeepXWsPublicFrame::Data { data, .. } = fresh else {
            anyhow::bail!("DeepX recovery requires a public data frame");
        };
        let page: DeepXPerpTradesPage = serde_json::from_str(data.get())?;
        for trade in page.items {
            let timestamp: jiff::Timestamp = trade.created_at.parse()?;
            if let Some((_, retained)) = self.recent.get(&trade.id) {
                anyhow::ensure!(
                    *retained == trade,
                    "DeepX fresh history conflicts with a retained execution"
                );
            }
            if timestamp.as_millisecond() >= i64::try_from(start)? {
                anyhow::ensure!(
                    by_id.get(&trade.id).is_some_and(|rest| **rest == trade),
                    "DeepX fresh trade page is missing or conflicting in REST recovery"
                );
            }
        }
        let mut next = self.clone();
        let emitted = next.apply_page(history.to_vec())?;
        *self = next;
        Ok(emitted)
    }

    fn apply_page(&mut self, trades: Vec<DeepXPerpTrade>) -> anyhow::Result<Vec<DeepXPerpTrade>> {
        let mut ids = BTreeSet::new();
        let mut previous = None;
        let mut validated = Vec::with_capacity(trades.len());
        for trade in trades {
            anyhow::ensure!(
                trade.market_id == u64::from(self.market_id) && trade.id != 0,
                "DeepX trade identity is invalid"
            );
            anyhow::ensure!(ids.insert(trade.id), "duplicate DeepX trade ID in page");
            anyhow::ensure!(
                trade.price > Decimal::ZERO && trade.size > Decimal::ZERO,
                "DeepX execution price and size must be positive"
            );
            anyhow::ensure!(
                matches!(trade.taker.as_str(), "Buyer" | "Seller"),
                "unknown DeepX trade taker"
            );
            let timestamp: jiff::Timestamp = trade.created_at.parse()?;
            let time = timestamp.as_millisecond();
            anyhow::ensure!(
                time >= 0 && jiff::Timestamp::from_millisecond(time)? == timestamp,
                "DeepX trade timestamp must be nonnegative and millisecond aligned"
            );
            anyhow::ensure!(
                previous.is_none_or(|previous| time <= previous),
                "DeepX trade page is not descending"
            );
            previous = Some(time);
            if let Some((_, retained)) = self.recent.get(&trade.id) {
                anyhow::ensure!(
                    *retained == trade,
                    "DeepX trade ID conflicts with retained execution"
                );
            } else if self.initialized {
                anyhow::ensure!(
                    self.watermark_ms.is_none_or(|watermark| time >= watermark),
                    "unseen older DeepX trade requires stream recovery"
                );
            }
            validated.push((time, trade));
        }
        validated.sort_by_key(|(time, trade)| (*time, trade.id));
        let mut next = self.clone();
        let mut emitted = Vec::new();
        for (time, trade) in validated {
            if next.recent.contains_key(&trade.id) {
                continue;
            }
            let watermark = next
                .watermark_ms
                .map_or(time, |watermark| watermark.max(time));
            next.watermark_ms = Some(watermark);
            if next.recent.len() == next.capacity.get() {
                let oldest = *next
                    .order
                    .front()
                    .ok_or_else(|| anyhow::anyhow!("DeepX trade identity queue is inconsistent"))?;
                let oldest_time = next
                    .recent
                    .get(&oldest)
                    .ok_or_else(|| anyhow::anyhow!("DeepX retained trade identity is missing"))?
                    .0;
                anyhow::ensure!(
                    oldest_time < watermark,
                    "DeepX equal-timestamp identity capacity exhausted"
                );
                next.order.pop_front();
                next.recent.remove(&oldest);
            }
            if self.initialized {
                emitted.push(trade.clone());
            }
            next.order.push_back(trade.id);
            next.recent.insert(trade.id, (time, trade));
        }
        next.initialized = true;
        *self = next;
        Ok(emitted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    fn trade(id: u64, second: u8) -> serde_json::Value {
        json!({"id":id,"marketId":2,"buyerOrderId":"10","buyer":"0x11",
            "sellerOrderId":"20","seller":"0x22","price": "77790.123456789012345678",
            "size":"0.0001","buyerLeverage":"25.0","sellerLeverage":"25.0",
            "createdAt":format!("2026-09-14T09:39:{second:02}.000Z"),
            "filledDirection":"Long","taker":"Buyer","takerFee":"0.001","makerFee":"-0.0001"})
    }

    fn frame(items: Vec<serde_json::Value>) -> DeepXWsPublicFrame {
        let items = serde_json::Value::Array(items);
        DeepXWsPublicFrame::parse(
            &json!({"type":"data","channel":"trades",
            "market":{"type":"perp","id":2},"timestamp":1789378785219_u64,
            "data":{"items":items,"hasNext":true}})
            .to_string(),
        )
        .unwrap()
    }

    #[rstest]
    fn initial_history_is_seeded_and_overlapping_pushes_emit_only_new_trades() {
        let mut stream = DeepXWsTradeStream::new(2, NonZeroUsize::new(4).unwrap());
        let initial = frame(vec![trade(2, 2), trade(1, 1)]);
        assert!(stream.ingest(&initial).unwrap().is_empty());
        assert!(stream.ingest(&initial).unwrap().is_empty());
        let new = stream
            .ingest(&frame(vec![trade(4, 4), trade(3, 3), trade(2, 2)]))
            .unwrap();
        assert_eq!(
            new.iter().map(|trade| trade.id).collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert_eq!(new[0].price.to_string(), "77790.123456789012345678");
        assert!(
            stream
                .ingest(&frame(vec![trade(4, 4), trade(3, 3)]))
                .unwrap()
                .is_empty()
        );
    }

    fn history(rows: &[(u64, u8)]) -> Vec<DeepXPerpTrade> {
        rows.iter()
            .map(|(id, second)| serde_json::from_value(trade(*id, *second)).unwrap())
            .collect()
    }

    #[rstest]
    fn recovery_emits_complete_gap_including_rows_absent_from_fresh_page() {
        let mut stream = DeepXWsTradeStream::new(2, NonZeroUsize::new(4).unwrap());
        stream
            .ingest(&frame(vec![trade(2, 2), trade(1, 1)]))
            .unwrap();
        let fresh = frame(vec![trade(7, 7), trade(6, 6), trade(1, 1)]);
        let (start, end) = stream.recovery_window(&fresh).unwrap();
        assert_eq!(end - start, 5000);
        let restored = stream
            .reconcile_history(
                &history(&[(7, 7), (6, 6), (5, 5), (4, 4), (3, 3), (2, 2)]),
                &fresh,
            )
            .unwrap();
        assert_eq!(
            restored.iter().map(|trade| trade.id).collect::<Vec<_>>(),
            vec![3, 4, 5, 6, 7]
        );
        assert_eq!(stream.recent.len(), 4);
        assert!(
            stream
                .ingest(&frame(vec![trade(7, 7), trade(6, 6)]))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            stream
                .ingest(&frame(vec![trade(8, 8), trade(7, 7)]))
                .unwrap()[0]
                .id,
            8
        );
    }

    #[rstest]
    fn recovery_preserves_all_boundary_ties_and_emits_new_tied_ids_once() {
        let mut stream = DeepXWsTradeStream::new(2, NonZeroUsize::new(4).unwrap());
        stream
            .ingest(&frame(vec![trade(2, 2), trade(1, 2)]))
            .unwrap();
        let fresh = frame(vec![trade(3, 2), trade(2, 2), trade(1, 2)]);
        let rest = history(&[(3, 2), (2, 2), (1, 2)]);
        assert_eq!(stream.reconcile_history(&rest, &fresh).unwrap()[0].id, 3);
        assert!(stream.reconcile_history(&rest, &fresh).unwrap().is_empty());
    }

    #[rstest]
    #[case("boundary-missing")]
    #[case("boundary-conflict")]
    #[case("fresh-missing")]
    #[case("fresh-conflict")]
    #[case("retained-old-conflict")]
    #[case("range")]
    #[case("ordering")]
    #[case("duplicate")]
    #[case("market")]
    #[case("negative-size")]
    #[case("timestamp")]
    #[case("tie-capacity")]
    fn recovery_failures_leave_every_state_field_unchanged(#[case] scenario: &str) {
        let mut stream = DeepXWsTradeStream::new(2, NonZeroUsize::new(4).unwrap());
        stream
            .ingest(&frame(vec![trade(2, 2), trade(1, 1)]))
            .unwrap();
        let before = stream.clone();
        let mut rest = history(&[(4, 4), (3, 3), (2, 2)]);
        let mut fresh_items = vec![trade(4, 4), trade(1, 1)];
        match scenario {
            "boundary-missing" => {
                rest.pop();
            }
            "boundary-conflict" => rest[2].price = Decimal::ONE,
            "fresh-missing" => {
                rest.remove(0);
            }
            "fresh-conflict" => rest[0].size = Decimal::ONE,
            "retained-old-conflict" => fresh_items[1]["price"] = json!("1"),
            "range" => rest.push(history(&[(9, 1)])[0].clone()),
            "ordering" => rest.swap(0, 1),
            "duplicate" => rest.push(rest[2].clone()),
            "market" => rest[1].market_id = 3,
            "negative-size" => rest[1].size = -Decimal::ONE,
            "timestamp" => rest[1].created_at = "2026-09-14T09:39:03.000001Z".into(),
            "tie-capacity" => {
                rest = history(&[(7, 3), (6, 3), (5, 3), (4, 3), (3, 3), (2, 2)]);
                fresh_items = vec![trade(7, 3), trade(6, 3)];
            }
            _ => unreachable!(),
        }
        assert!(
            stream
                .reconcile_history(&rest, &frame(fresh_items))
                .is_err()
        );
        assert_eq!(stream.recent, before.recent);
        assert_eq!(stream.order, before.order);
        assert_eq!(stream.watermark_ms, before.watermark_ms);
        assert_eq!(stream.initialized, before.initialized);
    }

    #[rstest]
    #[case("uninitialized")]
    #[case("empty-initial")]
    #[case("empty-fresh")]
    #[case("older-fresh")]
    fn recovery_requires_a_real_nonregressing_boundary(#[case] scenario: &str) {
        let mut stream = DeepXWsTradeStream::new(2, NonZeroUsize::new(4).unwrap());
        match scenario {
            "uninitialized" => {}
            "empty-initial" => {
                stream.ingest(&frame(vec![])).unwrap();
            }
            _ => {
                stream.ingest(&frame(vec![trade(2, 2)])).unwrap();
            }
        }
        let fresh = match scenario {
            "empty-fresh" => frame(vec![]),
            "older-fresh" => frame(vec![trade(1, 1)]),
            _ => frame(vec![trade(3, 3)]),
        };
        assert!(stream.recovery_window(&fresh).is_err());
    }

    #[rstest]
    #[case("price")]
    #[case("old")]
    #[case("ordering")]
    #[case("duplicate")]
    #[case("market")]
    #[case("timestamp")]
    fn invalid_pages_are_failure_atomic(#[case] mutation: &str) {
        let mut stream = DeepXWsTradeStream::new(2, NonZeroUsize::new(4).unwrap());
        stream
            .ingest(&frame(vec![trade(2, 2), trade(1, 1)]))
            .unwrap();
        let before = stream.clone();
        let mut items = vec![trade(3, 3), trade(2, 2)];
        match mutation {
            "price" => items[1]["price"] = json!("1"),
            "old" => items[1] = trade(9, 1),
            "ordering" => items.reverse(),
            "duplicate" => items[1] = items[0].clone(),
            "market" => items[0]["marketId"] = json!(3),
            _ => items[0]["createdAt"] = json!("2026-09-14T09:39:03.000001Z"),
        }
        assert!(stream.ingest(&frame(items)).is_err());
        assert_eq!(stream.recent, before.recent);
        assert_eq!(stream.order, before.order);
        assert_eq!(stream.watermark_ms, before.watermark_ms);
        assert_eq!(
            stream
                .ingest(&frame(vec![trade(3, 3), trade(2, 2)]))
                .unwrap()[0]
                .id,
            3
        );
    }

    #[rstest]
    fn bounded_eviction_preserves_timestamp_ties_and_rejects_old_replay() {
        let mut stream = DeepXWsTradeStream::new(2, NonZeroUsize::new(2).unwrap());
        stream
            .ingest(&frame(vec![trade(2, 2), trade(1, 1)]))
            .unwrap();
        assert_eq!(
            stream
                .ingest(&frame(vec![trade(3, 3), trade(2, 2)]))
                .unwrap()[0]
                .id,
            3
        );
        assert_eq!(stream.recent.len(), 2);
        assert!(stream.ingest(&frame(vec![trade(1, 1)])).is_err());
        assert_eq!(
            stream
                .ingest(&frame(vec![trade(4, 3), trade(3, 3)]))
                .unwrap()[0]
                .id,
            4
        );
        let before = stream.recent.clone();
        assert!(stream.ingest(&frame(vec![trade(5, 3)])).is_err());
        assert_eq!(stream.recent, before);
    }

    #[rstest]
    fn empty_initial_page_and_foreign_channel_handling() {
        let mut stream = DeepXWsTradeStream::new(2, NonZeroUsize::new(2).unwrap());
        assert!(stream.ingest(&frame(vec![])).unwrap().is_empty());
        assert_eq!(stream.ingest(&frame(vec![trade(1, 1)])).unwrap()[0].id, 1);
        let other = DeepXWsPublicFrame::Pong { timestamp: 1 };
        assert!(stream.ingest(&other).is_err());
    }
}
