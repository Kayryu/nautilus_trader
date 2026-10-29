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

//! Bounded SCALE decoding of DeepX account order and position storage.

use std::collections::BTreeSet;

use parity_scale_codec::{Compact, Decode, DecodeAll, Encode};

use super::storage::{
    DeepXActivePerpOrderIndexEntry, DeepXPerpOrderStorage, DeepXPerpPositionStorage,
    DeepXSpotOrderStorage, DeepXStoredOrderSide, DeepXStoredOrderStatus, DeepXStoredPerpOrderType,
};

const DEFAULT_MAX_PERP_POSITIONS: u32 = 8;
const ABSOLUTE_MAX_PERP_POSITIONS: u32 = 256;
const MAX_ACTIVE_ORDER_MARKETS: u32 = 256;
const MAX_ACTIVE_ORDERS: u32 = 4_096;

/// One active Spot order indexed by its deployment pair and side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXActiveSpotOrderIndexEntry {
    pub order_id: u64,
    pub pair: [u8; 32],
    pub is_buy: bool,
}

/// Decodes the active perpetual order index for one account.
#[must_use]
pub fn decode_active_perp_order_index(
    encoded: Option<&str>,
    subaccount: [u8; 20],
) -> Option<Vec<DeepXActivePerpOrderIndexEntry>> {
    let Some(encoded) = encoded else {
        return Some(Vec::new());
    };
    let bytes = decode_storage_hex(encoded)?;
    let mut input = bytes.as_slice();
    let Compact(market_count) = Compact::<u32>::decode(&mut input).ok()?;
    if market_count > MAX_ACTIVE_ORDER_MARKETS {
        return None;
    }
    let mut previous_market = None;
    let mut order_ids = BTreeSet::new();
    let mut entries = Vec::new();
    for _ in 0..market_count {
        let market_id = u16::decode(&mut input).ok()?;
        if previous_market.is_some_and(|previous| previous >= market_id) {
            return None;
        }
        previous_market = Some(market_id);
        let Compact(order_count) = Compact::<u32>::decode(&mut input).ok()?;
        let total = u32::try_from(entries.len())
            .ok()?
            .checked_add(order_count)?;
        if order_count == 0 || total > MAX_ACTIVE_ORDERS {
            return None;
        }
        entries.reserve(order_count as usize);
        for _ in 0..order_count {
            let entry = DeepXActivePerpOrderIndexEntry::decode(&mut input).ok()?;
            if entry.address != subaccount
                || entry.market_id != market_id
                || entry.created_at == 0
                || !order_ids.insert(entry.order_id)
            {
                return None;
            }
            entries.push(entry);
        }
    }
    input.is_empty().then_some(entries)
}

/// Decodes a bounded, canonical Spot order index with unique nonzero order IDs.
#[must_use]
pub fn decode_active_spot_order_index(
    encoded: Option<&str>,
) -> Option<Vec<DeepXActiveSpotOrderIndexEntry>> {
    let Some(encoded) = encoded else {
        return Some(Vec::new());
    };
    let bytes = decode_storage_hex(encoded)?;
    let mut input = bytes.as_slice();
    let Compact(pair_count) = Compact::<u32>::decode(&mut input).ok()?;
    if pair_count > MAX_ACTIVE_ORDER_MARKETS {
        return None;
    }
    let mut previous_pair = None;
    let mut order_ids = BTreeSet::new();
    let mut entries = Vec::new();
    for _ in 0..pair_count {
        let pair = <[u8; 32]>::decode(&mut input).ok()?;
        if previous_pair.is_some_and(|previous| previous >= pair) {
            return None;
        }
        previous_pair = Some(pair);
        let Compact(order_count) = Compact::<u32>::decode(&mut input).ok()?;
        let total = u32::try_from(entries.len())
            .ok()?
            .checked_add(order_count)?;
        if order_count == 0 || total > MAX_ACTIVE_ORDERS {
            return None;
        }
        entries.reserve(order_count as usize);
        for _ in 0..order_count {
            let order_id = u64::decode(&mut input).ok()?;
            let is_buy = bool::decode(&mut input).ok()?;
            if order_id == 0 || !order_ids.insert(order_id) {
                return None;
            }
            entries.push(DeepXActiveSpotOrderIndexEntry {
                order_id,
                pair,
                is_buy,
            });
        }
    }
    input.is_empty().then_some(entries)
}

/// Returns whether one Spot order agrees with its optional active-index entry.
#[must_use]
pub fn spot_order_matches_index(
    order: &DeepXSpotOrderStorage,
    order_id: u64,
    subaccount: [u8; 20],
    indexed: Option<&DeepXActiveSpotOrderIndexEntry>,
) -> bool {
    order.order_id == order_id
        && order.owner == subaccount
        && order.create_time != 0
        && order.status.is_active() == indexed.is_some()
        && indexed.is_none_or(|entry| {
            entry.order_id == order_id && entry.pair == order.pair && entry.is_buy == order.is_buy
        })
}

/// Decodes one active perpetual order corroborated by its account index entry.
#[must_use]
pub fn decode_active_perp_order(
    encoded: &str,
    index: &DeepXActivePerpOrderIndexEntry,
    subaccount: [u8; 20],
) -> Option<DeepXPerpOrderStorage> {
    let order = decode_order_storage(encoded)?;
    perp_order_matches_index(&order, index, subaccount).then_some(order)
}

/// Decodes one perpetual order by ID and checks its active or terminal state.
#[must_use]
pub fn decode_perp_order_by_id(
    encoded: &str,
    order_id: u64,
    subaccount: [u8; 20],
    active_entry: Option<&DeepXActivePerpOrderIndexEntry>,
) -> Option<DeepXPerpOrderStorage> {
    let order: DeepXPerpOrderStorage = decode_order_storage(encoded)?;
    if order.order_id != order_id
        || order.owner != subaccount
        || order.create_time == 0
        || order.size == 0
        || order.size_filled.checked_add(order.size_remain) != Some(order.size)
        || order.status.is_active() != active_entry.is_some()
        || active_entry.is_some_and(|entry| !perp_order_matches_index(&order, entry, subaccount))
    {
        return None;
    }
    let status_quantities_match = match order.status {
        DeepXStoredOrderStatus::Open => order.size_filled == 0 && order.size_remain == order.size,
        DeepXStoredOrderStatus::PartiallyFilled => order.size_filled > 0 && order.size_remain > 0,
        DeepXStoredOrderStatus::Filled => order.size_filled == order.size && order.size_remain == 0,
        DeepXStoredOrderStatus::Canceled
        | DeepXStoredOrderStatus::Rejected
        | DeepXStoredOrderStatus::Expired => true,
    };
    status_quantities_match.then_some(order)
}

/// Decodes one Spot order by ID and checks its active-index status.
#[must_use]
pub fn decode_spot_order_by_id(
    encoded: &str,
    order_id: u64,
    subaccount: [u8; 20],
    active_entry: Option<&DeepXActiveSpotOrderIndexEntry>,
) -> Option<DeepXSpotOrderStorage> {
    let order = decode_order_storage(encoded)?;
    spot_order_matches_index(&order, order_id, subaccount, active_entry).then_some(order)
}

/// Returns whether an active perpetual order agrees with its account index entry.
#[must_use]
pub fn perp_order_matches_index(
    order: &DeepXPerpOrderStorage,
    index: &DeepXActivePerpOrderIndexEntry,
    subaccount: [u8; 20],
) -> bool {
    let expected_long = matches!(index.order_side, DeepXStoredOrderSide::Buy);
    order.order_id == index.order_id
        && order.owner == subaccount
        && index.address == subaccount
        && order.market_id == index.market_id
        && order.is_long == expected_long
        && order.order_type == index.order_type
        && order.price == index.price
        && order.create_time == index.created_at
        && order.create_time != 0
        && order.leverage != 0
        && order.status.is_active()
        && order.size != 0
        && order.size_remain != 0
        && order.size_filled.checked_add(order.size_remain) == Some(order.size)
        && (!matches!(order.order_type, DeepXStoredPerpOrderType::Limit(_)) || order.price != 0)
}

/// Decodes the runtime's perpetual position limit, including its storage default.
#[must_use]
pub fn decode_max_perp_positions(encoded: Option<&str>) -> Option<u32> {
    let maximum = match encoded {
        None => DEFAULT_MAX_PERP_POSITIONS,
        Some(encoded) => {
            let bytes = decode_storage_hex(encoded)?;
            let mut input = bytes.as_slice();
            let value = u32::decode(&mut input).ok()?;
            if !input.is_empty() {
                return None;
            }
            value
        }
    };
    (maximum > 0 && maximum <= ABSOLUTE_MAX_PERP_POSITIONS).then_some(maximum)
}

/// Decodes complete, unique perpetual positions belonging to one account.
#[must_use]
pub fn decode_perp_positions(
    encoded: Option<&str>,
    subaccount: [u8; 20],
    maximum: u32,
) -> Option<Vec<DeepXPerpPositionStorage>> {
    if maximum == 0 || maximum > ABSOLUTE_MAX_PERP_POSITIONS {
        return None;
    }
    let Some(encoded) = encoded else {
        return Some(Vec::new());
    };
    let bytes = decode_storage_hex(encoded)?;
    let mut input = bytes.as_slice();
    let Compact(length) = Compact::<u32>::decode(&mut input).ok()?;
    if length > maximum {
        return None;
    }
    let mut market_ids = BTreeSet::new();
    let mut positions = Vec::with_capacity(length as usize);
    for _ in 0..length {
        let position = DeepXPerpPositionStorage::decode(&mut input).ok()?;
        if position.owner != subaccount
            || position.base_asset_amount == 0
            || position.entry_price == 0
            || position.leverage == 0
            || position.last_settle_price == 0
            || !market_ids.insert(position.market_id)
        {
            return None;
        }
        positions.push(position);
    }
    if !input.is_empty() {
        return None;
    }
    positions.sort_unstable_by_key(|position| position.market_id);
    Some(positions)
}

fn decode_storage_hex(encoded: &str) -> Option<Vec<u8>> {
    encoded
        .strip_prefix("0x")
        .and_then(|value| hex::decode(value).ok())
}

fn decode_order_storage<T: Decode>(encoded: &str) -> Option<T> {
    let bytes = decode_storage_hex(encoded)?;
    T::decode_all(&mut &bytes[..]).ok()
}

fn storage_value_key(pallet: &[u8], item: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(32);
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(pallet));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(item));
    encoded
}

/// Returns the key for the runtime's maximum perpetual position count.
#[must_use]
pub fn subaccount_max_perp_positions_storage_key() -> String {
    format!(
        "0x{}",
        hex::encode(storage_value_key(b"Subaccount", b"MaxPerpPositions")),
    )
}

/// Returns the key for one perpetual market's runtime storage.
#[must_use]
pub fn perp_market_storage_key(market_id: u16) -> String {
    let mut encoded = storage_value_key(b"PerpMarket", b"PerpMarkets");
    encoded.extend_from_slice(&market_id.encode());
    format!("0x{}", hex::encode(encoded))
}

/// Returns the key for one Spot market's runtime storage.
#[must_use]
pub fn spot_market_storage_key(pair: [u8; 32]) -> String {
    let mut encoded = storage_value_key(b"SpotMarket", b"SpotMarkets");
    encoded.extend_from_slice(&pair);
    format!("0x{}", hex::encode(encoded))
}

/// Returns the key for one account's perpetual positions.
#[must_use]
pub fn user_perp_positions_storage_key(subaccount: [u8; 20]) -> String {
    let mut encoded = storage_value_key(b"PerpMarket", b"UserPerpPositions");
    encoded.extend_from_slice(&subaccount);
    format!("0x{}", hex::encode(encoded))
}

/// Returns the key for one account's active perpetual order index.
#[must_use]
pub fn active_perp_orders_storage_key(subaccount: [u8; 20]) -> String {
    let mut encoded = storage_value_key(b"PerpMarket", b"ActivePerpOrdersFor");
    encoded.extend_from_slice(&subaccount);
    format!("0x{}", hex::encode(encoded))
}

/// Returns the key for one account's active Spot order index.
#[must_use]
pub fn active_spot_orders_storage_key(subaccount: [u8; 20]) -> String {
    let mut encoded = storage_value_key(b"SpotMarket", b"ActiveSpotOrdersFor");
    encoded.extend_from_slice(&subaccount);
    format!("0x{}", hex::encode(encoded))
}

/// Returns the key for one account's Spot order by ID.
#[must_use]
pub fn spot_order_info_storage_key(subaccount: [u8; 20], order_id: u64) -> String {
    let mut encoded = storage_value_key(b"SpotMarket", b"OrderInfo");
    encoded.extend_from_slice(&subaccount);
    encoded.extend_from_slice(&order_id.encode());
    format!("0x{}", hex::encode(encoded))
}

/// Returns the key for one account's perpetual order by ID.
#[must_use]
pub fn perp_order_info_storage_key(subaccount: [u8; 20], order_id: u64) -> String {
    let mut encoded = storage_value_key(b"PerpMarket", b"OrderInfo");
    encoded.extend_from_slice(&subaccount);
    encoded.extend_from_slice(&order_id.encode());
    format!("0x{}", hex::encode(encoded))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use parity_scale_codec::Encode;
    use rstest::rstest;

    use super::*;
    use crate::rpc::storage::{
        DeepXStoredOrderStatus, DeepXStoredPostOnlyParam, DeepXStoredSpotOrderType,
        DeepXStoredTimeInForce,
    };

    fn position(account: [u8; 20], market_id: u16) -> DeepXPerpPositionStorage {
        DeepXPerpPositionStorage {
            market_id,
            is_long: true,
            base_asset_amount: 25,
            entry_price: 2_500,
            leverage: 12,
            last_funding_rate: -1,
            version: 7,
            realized_pnl: 5,
            funding_payment: -2,
            owner: account,
            take_profit: Some(3_000),
            stop_loss: None,
            last_settle_price: 2_550,
        }
    }

    fn index_entry(
        account: [u8; 20],
        market_id: u16,
        order_id: u64,
    ) -> DeepXActivePerpOrderIndexEntry {
        DeepXActivePerpOrderIndexEntry {
            address: account,
            market_id,
            order_side: DeepXStoredOrderSide::Buy,
            order_type: DeepXStoredPerpOrderType::Stop,
            order_id,
            price: 2_500,
            created_at: 1,
        }
    }

    fn order(account: [u8; 20], order_id: u64) -> DeepXPerpOrderStorage {
        DeepXPerpOrderStorage {
            order_id,
            owner: account,
            market_id: 2,
            is_long: true,
            size: 25,
            price: 2_500,
            order_type: DeepXStoredPerpOrderType::Limit(DeepXStoredTimeInForce::Gtc),
            create_time: 1,
            leverage: 12,
            status: DeepXStoredOrderStatus::Open,
            size_filled: 5,
            size_remain: 20,
            take_profit: None,
            stop_loss: None,
            reduce_only: false,
            post_only: DeepXStoredPostOnlyParam::None,
        }
    }

    #[rstest]
    fn perp_order_index_match_rejects_contradictory_storage() {
        let account = [0x11; 20];
        let stored_order = order(account, 10);
        let mut index = index_entry(account, 2, 10);
        index.order_type = stored_order.order_type.clone();
        assert!(perp_order_matches_index(&stored_order, &index, account));
        let generated = order(account, 0);
        assert!(perp_order_matches_index(
            &generated,
            &index_entry_for_order(&generated),
            account,
        ));

        let mut invalid = stored_order.clone();
        invalid.status = DeepXStoredOrderStatus::Filled;
        assert!(!perp_order_matches_index(&invalid, &index, account));
        invalid = stored_order.clone();
        invalid.size_remain -= 1;
        assert!(!perp_order_matches_index(&invalid, &index, account));
        invalid = stored_order.clone();
        invalid.owner = [0x22; 20];
        assert!(!perp_order_matches_index(&invalid, &index, account));
        invalid = stored_order;
        invalid.price = 0;
        index.price = 0;
        assert!(!perp_order_matches_index(&invalid, &index, account));
        let mut foreign_index = index_entry_for_order(&order(account, 10));
        foreign_index.address = [0x22; 20];
        assert!(!perp_order_matches_index(
            &order(account, 10),
            &foreign_index,
            account
        ));
    }

    #[rstest]
    fn active_perp_order_decoder_requires_complete_matching_storage() {
        let account = [0x11; 20];
        let order = order(account, 10);
        let index = index_entry_for_order(&order);
        let encoded = format!("0x{}", hex::encode(order.encode()));

        assert_eq!(
            decode_active_perp_order(&encoded, &index, account),
            Some(order)
        );
        assert_eq!(
            decode_active_perp_order(&format!("{encoded}00"), &index, account),
            None
        );
        assert_eq!(decode_active_perp_order("0x", &index, account), None);
        assert_eq!(decode_active_perp_order(&encoded, &index, [0x22; 20]), None);
        let mismatched = DeepXActivePerpOrderIndexEntry { price: 1, ..index };
        assert_eq!(
            decode_active_perp_order(&encoded, &mismatched, account),
            None
        );
    }

    #[rstest]
    fn perp_order_by_id_decoder_checks_status_quantities_and_index() {
        let account = [0x11; 20];
        let mut stored_order = order(account, 10);
        let index = index_entry_for_order(&stored_order);
        let encoded = format!("0x{}", hex::encode(stored_order.encode()));
        assert_eq!(
            decode_perp_order_by_id(&encoded, 10, account, Some(&index)),
            None
        );

        stored_order.status = DeepXStoredOrderStatus::PartiallyFilled;
        let partial = format!("0x{}", hex::encode(stored_order.encode()));
        assert_eq!(
            decode_perp_order_by_id(&partial, 10, account, Some(&index)),
            Some(stored_order.clone()),
        );
        assert_eq!(decode_perp_order_by_id(&partial, 10, account, None), None);
        assert_eq!(
            decode_perp_order_by_id(&partial, 11, account, Some(&index)),
            None
        );
        assert_eq!(
            decode_perp_order_by_id(&partial, 10, [0x22; 20], Some(&index)),
            None
        );
        assert_eq!(
            decode_perp_order_by_id(&format!("{partial}00"), 10, account, Some(&index)),
            None
        );

        stored_order.status = DeepXStoredOrderStatus::Filled;
        stored_order.size_filled = stored_order.size;
        stored_order.size_remain = 0;
        let filled = format!("0x{}", hex::encode(stored_order.encode()));
        assert_eq!(
            decode_perp_order_by_id(&filled, 10, account, None),
            Some(stored_order.clone())
        );
        assert_eq!(
            decode_perp_order_by_id(&filled, 10, account, Some(&index)),
            None
        );

        stored_order.size_filled -= 1;
        stored_order.size_remain = 1;
        let invalid = format!("0x{}", hex::encode(stored_order.encode()));
        assert_eq!(decode_perp_order_by_id(&invalid, 10, account, None), None);
    }

    fn index_entry_for_order(order: &DeepXPerpOrderStorage) -> DeepXActivePerpOrderIndexEntry {
        DeepXActivePerpOrderIndexEntry {
            address: order.owner,
            market_id: order.market_id,
            order_side: DeepXStoredOrderSide::Buy,
            order_type: order.order_type.clone(),
            order_id: order.order_id,
            price: order.price,
            created_at: order.create_time,
        }
    }

    fn spot_order(account: [u8; 20], order_id: u64) -> DeepXSpotOrderStorage {
        DeepXSpotOrderStorage {
            order_id,
            owner: account,
            pair: [0x11; 32],
            price: Some([0x22; 32]),
            quote_amount: [0x33; 32],
            base_amount: [0x44; 32],
            create_time: 1,
            status: DeepXStoredOrderStatus::Open,
            order_type: DeepXStoredSpotOrderType::Limit(DeepXStoredTimeInForce::Gtc),
            post_only: DeepXStoredPostOnlyParam::None,
            reduce_only: false,
            is_buy: true,
        }
    }

    #[rstest]
    fn active_spot_order_index_requires_canonical_pairs_and_unique_nonzero_ids() {
        let encoded = BTreeMap::from([
            ([0x11_u8; 32], vec![(10_u64, true)]),
            ([0x22_u8; 32], vec![(11, false)]),
        ])
        .encode();
        let encoded = format!("0x{}", hex::encode(&encoded));
        assert_eq!(
            decode_active_spot_order_index(Some(&encoded)),
            Some(vec![
                DeepXActiveSpotOrderIndexEntry {
                    order_id: 10,
                    pair: [0x11; 32],
                    is_buy: true,
                },
                DeepXActiveSpotOrderIndexEntry {
                    order_id: 11,
                    pair: [0x22; 32],
                    is_buy: false,
                },
            ])
        );
        assert_eq!(decode_active_spot_order_index(None), Some(vec![]));
        assert_eq!(
            decode_active_spot_order_index(Some(&format!("{encoded}00"))),
            None
        );
        for invalid in [
            BTreeMap::from([([0x11_u8; 32], vec![(0_u64, true)])]).encode(),
            BTreeMap::from([
                ([0x11_u8; 32], vec![(10_u64, true)]),
                ([0x22_u8; 32], vec![(10, false)]),
            ])
            .encode(),
            BTreeMap::from([([0x11_u8; 32], Vec::<(u64, bool)>::new())]).encode(),
            Compact(MAX_ACTIVE_ORDER_MARKETS + 1).encode(),
        ] {
            assert_eq!(
                decode_active_spot_order_index(Some(&format!("0x{}", hex::encode(invalid)))),
                None
            );
        }
    }

    #[rstest]
    fn spot_order_index_match_rejects_contradictory_storage() {
        let account = [0x11; 20];
        let order = spot_order(account, 10);
        let index = DeepXActiveSpotOrderIndexEntry {
            order_id: 10,
            pair: order.pair,
            is_buy: true,
        };
        assert!(spot_order_matches_index(&order, 10, account, Some(&index)));
        assert!(!spot_order_matches_index(&order, 10, account, None));
        assert!(!spot_order_matches_index(&order, 11, account, Some(&index)));
        assert!(!spot_order_matches_index(
            &order,
            10,
            [0x22; 20],
            Some(&index)
        ));
        let wrong_side = DeepXActiveSpotOrderIndexEntry {
            is_buy: false,
            ..index
        };
        assert!(!spot_order_matches_index(
            &order,
            10,
            account,
            Some(&wrong_side)
        ));
        let mut finished = order;
        finished.status = DeepXStoredOrderStatus::Filled;
        assert!(spot_order_matches_index(&finished, 10, account, None));
        assert!(!spot_order_matches_index(
            &finished,
            10,
            account,
            Some(&index)
        ));
        finished.create_time = 0;
        assert!(!spot_order_matches_index(&finished, 10, account, None));
    }

    #[rstest]
    fn spot_order_by_id_decoder_requires_complete_matching_storage() {
        let account = [0x11; 20];
        let order = spot_order(account, 10);
        let index = DeepXActiveSpotOrderIndexEntry {
            order_id: 10,
            pair: order.pair,
            is_buy: order.is_buy,
        };
        let encoded = format!("0x{}", hex::encode(order.encode()));
        assert_eq!(
            decode_spot_order_by_id(&encoded, 10, account, Some(&index)),
            Some(order)
        );
        assert_eq!(
            decode_spot_order_by_id(&format!("{encoded}00"), 10, account, Some(&index)),
            None
        );
        assert_eq!(
            decode_spot_order_by_id("0x", 10, account, Some(&index)),
            None
        );
        assert_eq!(decode_spot_order_by_id(&encoded, 10, account, None), None);
        assert_eq!(
            decode_spot_order_by_id(&encoded, 11, account, Some(&index)),
            None
        );
    }

    #[rstest]
    fn storage_keys_use_runtime_identity_map_encoding() {
        let account = [0x11; 20];
        let perp_index = active_perp_orders_storage_key(account);
        let spot_index = active_spot_orders_storage_key(account);
        let perp_order = perp_order_info_storage_key(account, 0x0102_0304_0506_0708);
        let spot_order = spot_order_info_storage_key(account, 0x0102_0304_0506_0708);
        let maximum = subaccount_max_perp_positions_storage_key();
        let positions = user_perp_positions_storage_key(account);

        assert_eq!(maximum.len(), 66);
        assert_eq!(positions.len(), 106);
        assert_eq!(perp_index.len(), 106);
        assert_eq!(spot_index.len(), 106);
        assert_eq!(perp_order.len(), 122);
        assert_eq!(spot_order.len(), 122);
        for key in [&positions, &perp_index, &spot_index] {
            assert!(key.ends_with(&hex::encode(account)));
        }
        assert!(perp_order.ends_with("0807060504030201"));
        assert!(spot_order.ends_with("0807060504030201"));
        assert_ne!(perp_order, perp_order_info_storage_key(account, 2));
        assert_ne!(positions, user_perp_positions_storage_key([0x22; 20]));
        assert_ne!(perp_index, spot_index);
        assert_eq!(
            perp_market_storage_key(3),
            "0xb052a07884e121f56b64aa711c766532bdc7a97672ba816c4074df5f464124630300",
        );
        assert_eq!(spot_market_storage_key([0x11; 32]).len(), 130);
    }

    #[rstest]
    fn active_perp_order_index_requires_bounded_scoped_canonical_entries() {
        let account = [0x11; 20];
        let first = index_entry(account, 2, 10);
        let second = index_entry(account, 3, 11);
        let encoded =
            BTreeMap::from([(2_u16, vec![first.clone()]), (3, vec![second.clone()])]).encode();
        let encoded = format!("0x{}", hex::encode(&encoded));

        assert_eq!(
            decode_active_perp_order_index(Some(&encoded), account),
            Some(vec![first.clone(), second])
        );
        assert_eq!(decode_active_perp_order_index(None, account), Some(vec![]));
        assert_eq!(
            decode_active_perp_order_index(Some(&format!("{encoded}00")), account),
            None
        );
        assert_eq!(
            decode_active_perp_order_index(Some(&encoded), [0x22; 20]),
            None
        );

        let duplicate =
            BTreeMap::from([(2_u16, vec![first]), (3, vec![index_entry(account, 3, 10)])]).encode();
        assert_eq!(
            decode_active_perp_order_index(Some(&format!("0x{}", hex::encode(duplicate))), account),
            None
        );
        let empty_market =
            BTreeMap::from([(2_u16, Vec::<DeepXActivePerpOrderIndexEntry>::new())]).encode();
        assert_eq!(
            decode_active_perp_order_index(
                Some(&format!("0x{}", hex::encode(empty_market))),
                account
            ),
            None
        );
        let excessive = Compact(MAX_ACTIVE_ORDER_MARKETS + 1).encode();
        assert_eq!(
            decode_active_perp_order_index(Some(&format!("0x{}", hex::encode(excessive))), account),
            None
        );
    }

    #[rstest]
    fn max_perp_positions_requires_one_bounded_runtime_value() {
        assert_eq!(decode_max_perp_positions(None), Some(8));
        assert_eq!(decode_max_perp_positions(Some("0x0c000000")), Some(12));
        for encoded in ["0x", "0x00000000", "0x01010000", "0x0100000000"] {
            assert_eq!(decode_max_perp_positions(Some(encoded)), None);
        }
    }

    #[rstest]
    fn perp_positions_require_bounded_scoped_unique_complete_values() {
        let account = [0x11; 20];
        let first = position(account, 3);
        let second = position(account, 2);
        let encoded = format!(
            "0x{}",
            hex::encode(vec![first.clone(), second.clone()].encode())
        );
        assert_eq!(
            decode_perp_positions(Some(&encoded), account, 8),
            Some(vec![second, first.clone()])
        );
        assert_eq!(decode_perp_positions(None, account, 8), Some(vec![]));
        assert_eq!(decode_perp_positions(Some(&encoded), account, 1), None);
        assert_eq!(decode_perp_positions(Some(&encoded), account, 0), None);
        assert_eq!(decode_perp_positions(Some(&encoded), account, 257), None);
        assert_eq!(
            decode_perp_positions(Some(&format!("{encoded}00")), account, 8),
            None
        );
        assert_eq!(decode_perp_positions(Some(&encoded), [0x22; 20], 8), None);
        let duplicate = format!(
            "0x{}",
            hex::encode(vec![first.clone(), first.clone()].encode())
        );
        assert_eq!(decode_perp_positions(Some(&duplicate), account, 8), None);
        let mut zero = first;
        zero.base_asset_amount = 0;
        let zero = format!("0x{}", hex::encode(vec![zero].encode()));
        assert_eq!(decode_perp_positions(Some(&zero), account, 8), None);
    }
}
