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

//! Order book sequence synchronization for DeepX WebSocket updates.

use std::collections::{HashMap, HashSet};

use super::messages::DeepXOrderBookUpdate;

/// Result of validating an order book update against local sequence state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXBookSyncOutcome {
    /// The update is continuous and can be published.
    Accept,
    /// The update must be dropped while awaiting a valid snapshot.
    Suppress,
    /// The update exposed a sequence gap and recovery must be requested.
    Recover,
}

/// Tracks DeepX order book sequence continuity independently for each raw symbol.
#[derive(Debug, Default)]
pub struct DeepXBookSync {
    last_update_ids: HashMap<String, u64>,
    recovering: HashSet<String>,
}

impl DeepXBookSync {
    /// Validates an update and advances sequence state when it is continuous.
    pub fn validate(&mut self, update: &DeepXOrderBookUpdate) -> DeepXBookSyncOutcome {
        let symbol = update.symbol.as_str();

        if update.follows(None) {
            self.last_update_ids
                .insert(symbol.to_owned(), update.last_update_id);
            self.recovering.remove(symbol);
            return DeepXBookSyncOutcome::Accept;
        }

        if self.recovering.contains(symbol) {
            return DeepXBookSyncOutcome::Suppress;
        }

        let last_update_id = self.last_update_ids.get(symbol).copied();
        if update.follows(last_update_id) {
            self.last_update_ids
                .insert(symbol.to_owned(), update.last_update_id);
            DeepXBookSyncOutcome::Accept
        } else {
            self.last_update_ids.remove(symbol);
            self.recovering.insert(symbol.to_owned());
            DeepXBookSyncOutcome::Recover
        }
    }

    /// Removes sequence state for a symbol after unsubscription or disconnect.
    pub fn reset(&mut self, symbol: &str) {
        self.last_update_ids.remove(symbol);
        self.recovering.remove(symbol);
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::websocket::messages::DeepXWsMessage;

    fn update(payload: &str) -> DeepXOrderBookUpdate {
        serde_json::from_str::<DeepXWsMessage<DeepXOrderBookUpdate>>(payload)
            .unwrap()
            .data
    }

    #[rstest]
    fn accepts_continuous_updates_and_recovers_from_gap() {
        let snapshot = update(
            r#"{"channel":"perp@orderbook","data":{"asks":[],"bids":[],"engineTime":1,"lastUpdateId":10,"prevLastUpdateId":null,"serverTime":1,"symbol":"ETH-USDC","updateType":"snapshot"}}"#,
        );
        let continuous = update(
            r#"{"channel":"perp@orderbook","data":{"asks":[],"bids":[["2456.81","0.291"]],"engineTime":2,"lastUpdateId":11,"prevLastUpdateId":10,"serverTime":2,"symbol":"ETH-USDC","updateType":"delta"}}"#,
        );
        let gap = update(
            r#"{"channel":"perp@orderbook","data":{"asks":[],"bids":[["2456.82","0.100"]],"engineTime":3,"lastUpdateId":13,"prevLastUpdateId":12,"serverTime":3,"symbol":"ETH-USDC","updateType":"delta"}}"#,
        );
        let after_gap = update(
            r#"{"channel":"perp@orderbook","data":{"asks":[],"bids":[["2456.83","0.100"]],"engineTime":4,"lastUpdateId":14,"prevLastUpdateId":13,"serverTime":4,"symbol":"ETH-USDC","updateType":"delta"}}"#,
        );
        let recovered = update(
            r#"{"channel":"perp@orderbook","data":{"asks":[],"bids":[],"engineTime":5,"lastUpdateId":20,"prevLastUpdateId":null,"serverTime":5,"symbol":"ETH-USDC","updateType":"snapshot"}}"#,
        );
        let mut sync = DeepXBookSync::default();

        assert_eq!(sync.validate(&snapshot), DeepXBookSyncOutcome::Accept);
        assert_eq!(sync.validate(&continuous), DeepXBookSyncOutcome::Accept);
        assert_eq!(sync.validate(&gap), DeepXBookSyncOutcome::Recover);
        assert_eq!(sync.validate(&after_gap), DeepXBookSyncOutcome::Suppress);
        assert_eq!(sync.validate(&recovered), DeepXBookSyncOutcome::Accept);
    }

    #[rstest]
    fn requests_recovery_for_delta_before_snapshot() {
        let delta = update(
            r#"{"channel":"perp@orderbook","data":{"asks":[],"bids":[["2456.81","0.291"]],"engineTime":2,"lastUpdateId":11,"prevLastUpdateId":10,"serverTime":2,"symbol":"ETH-USDC","updateType":"delta"}}"#,
        );
        let mut sync = DeepXBookSync::default();

        assert_eq!(sync.validate(&delta), DeepXBookSyncOutcome::Recover);
        assert_eq!(sync.validate(&delta), DeepXBookSyncOutcome::Suppress);
    }

    #[rstest]
    fn tracks_symbols_independently_and_resets_one() {
        let eth_snapshot = update(
            r#"{"channel":"perp@orderbook","data":{"asks":[],"bids":[],"engineTime":1,"lastUpdateId":10,"prevLastUpdateId":null,"serverTime":1,"symbol":"ETH-USDC","updateType":"snapshot"}}"#,
        );
        let btc_snapshot = update(
            r#"{"channel":"perp@orderbook","data":{"asks":[],"bids":[],"engineTime":1,"lastUpdateId":20,"prevLastUpdateId":null,"serverTime":1,"symbol":"BTC-USDC","updateType":"snapshot"}}"#,
        );
        let btc_delta = update(
            r#"{"channel":"perp@orderbook","data":{"asks":[],"bids":[["100000","0.001"]],"engineTime":2,"lastUpdateId":21,"prevLastUpdateId":20,"serverTime":2,"symbol":"BTC-USDC","updateType":"delta"}}"#,
        );
        let mut sync = DeepXBookSync::default();

        assert_eq!(sync.validate(&eth_snapshot), DeepXBookSyncOutcome::Accept);
        assert_eq!(sync.validate(&btc_snapshot), DeepXBookSyncOutcome::Accept);
        sync.reset("ETH-USDC");

        assert_eq!(sync.validate(&btc_delta), DeepXBookSyncOutcome::Accept);
        assert_eq!(sync.validate(&eth_snapshot), DeepXBookSyncOutcome::Accept);
    }
}
