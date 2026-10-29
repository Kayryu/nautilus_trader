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

//! DeepX runtime event storage keys and complete batch assembly.

use parity_scale_codec::{Compact, Decode, Encode};

/// Storage key for the runtime's unsharded `System.Events` value.
pub const SYSTEM_EVENTS_STORAGE_KEY: &str = concat!(
    "0x26aa394eea5630e07c48ae0c9558cef7",
    "80d41e5e16056765bc8461851072c9d7",
);

const SYSTEM_STORAGE_PREFIX: &str = "System";
const SYSTEM_THREADS_STORAGE_ITEM: &str = "Threads";
const SYSTEM_EVENTS_MAP_STORAGE_ITEM: &str = "EventsMap";

/// Decodes one nonempty 0x-prefixed event storage value.
#[must_use]
pub fn decode_event_storage_value(encoded: &str) -> Option<Vec<u8>> {
    let value = encoded
        .strip_prefix("0x")
        .filter(|value| !value.is_empty())?;
    hex::decode(value).ok()
}

/// Decodes the exact last-thread byte for a sharded event storage value.
#[must_use]
pub fn decode_event_threads(encoded: &str) -> Option<u8> {
    let bytes = decode_event_storage_value(encoded)?;
    let [last_thread] = bytes.as_slice() else {
        return None;
    };
    Some(*last_thread)
}

/// Returns the key for the event thread count at a runtime block number.
#[must_use]
pub fn system_threads_storage_key(block_number: u64) -> String {
    system_map_storage_key(
        SYSTEM_STORAGE_PREFIX,
        SYSTEM_THREADS_STORAGE_ITEM,
        &block_number.to_le_bytes(),
    )
}

/// Returns the key for one shard of a runtime block's events.
#[must_use]
pub fn system_events_map_storage_key(block_number: u64, thread: u8) -> String {
    let block_key = sp_crypto_hashing::blake2_128(&block_number.to_le_bytes());
    let thread_key = sp_crypto_hashing::blake2_128(&[thread]);
    let mut encoded = Vec::with_capacity(64);
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(
        SYSTEM_STORAGE_PREFIX.as_bytes(),
    ));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(
        SYSTEM_EVENTS_MAP_STORAGE_ITEM.as_bytes(),
    ));
    encoded.extend_from_slice(&block_key);
    encoded.extend_from_slice(&thread_key);
    format!("0x{}", hex::encode(encoded))
}

/// Combines complete event-map shards into one `System.Events` SCALE value.
#[must_use]
pub fn combine_events_map_batches(batches: &[Vec<u8>]) -> Option<Vec<u8>> {
    let mut records = Vec::new();
    let mut total = 0_u32;
    for batch in batches {
        let mut input = batch.as_slice();
        let count = Compact::<u32>::decode(&mut input).ok()?.0;
        if (input.is_empty() && count != 0) || (!input.is_empty() && count == 0) {
            return None;
        }
        total = total.checked_add(count)?;
        records.extend_from_slice(input);
    }
    let mut combined = Compact(total).encode();
    combined.extend_from_slice(&records);
    Some(combined)
}

fn system_map_storage_key(prefix: &str, item: &str, key: &[u8]) -> String {
    let mut encoded = Vec::with_capacity(32 + key.len());
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(prefix.as_bytes()));
    encoded.extend_from_slice(&sp_crypto_hashing::twox_128(item.as_bytes()));
    encoded.extend_from_slice(key);
    format!("0x{}", hex::encode(encoded))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn events_map_batches_combine_compact_counts() {
        let mut first = Compact(1_u32).encode();
        first.push(10);
        let mut second = Compact(2_u32).encode();
        second.extend_from_slice(&[20, 30]);
        assert_eq!(
            combine_events_map_batches(&[first, second]),
            Some([Compact(3_u32).encode(), vec![10, 20, 30]].concat()),
        );
        assert_eq!(
            combine_events_map_batches(&[]),
            Some(Compact(0_u32).encode())
        );
    }

    #[rstest]
    fn events_map_batches_reject_malformed_counts() {
        let mut zero_count = Compact(0_u32).encode();
        zero_count.push(1);
        let nonzero_count = Compact(1_u32).encode();
        assert_eq!(combine_events_map_batches(&[zero_count]), None);
        assert_eq!(combine_events_map_batches(&[nonzero_count]), None);
        assert_eq!(combine_events_map_batches(&[Vec::new()]), None);
    }

    #[rstest]
    fn event_storage_hex_and_thread_count_are_exact() {
        assert_eq!(decode_event_storage_value("0x0102"), Some(vec![1, 2]));
        assert_eq!(decode_event_storage_value("0x"), None);
        assert_eq!(decode_event_storage_value("0xzz"), None);
        assert_eq!(decode_event_storage_value("0102"), None);
        assert_eq!(decode_event_threads("0x02"), Some(2));
        assert_eq!(decode_event_threads("0x0203"), None);
        assert_eq!(decode_event_threads("0x"), None);
    }

    #[rstest]
    fn event_storage_keys_bind_block_and_thread() {
        assert_eq!(SYSTEM_EVENTS_STORAGE_KEY.len(), 66);
        let threads = system_threads_storage_key(42);
        let first = system_events_map_storage_key(42, 0);
        assert_eq!(threads.len(), 82);
        assert_eq!(first.len(), 130);
        assert_ne!(threads, system_threads_storage_key(43));
        assert_ne!(first, system_events_map_storage_key(42, 1));
        assert_ne!(first, system_events_map_storage_key(43, 0));
    }
}
