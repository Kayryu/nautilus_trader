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

//! Timestamp-nonce allocation for the DeepX ordinary placement priority group.

use std::collections::BTreeSet;

use anyhow::Context;
use deepx_rust_sdk::{
    rpc::{DeepXRpcClient, timestamp::query_timestamp_now_at_block},
    signing::DeepXRuntimeConfig,
};
use subxt_core::{
    Metadata,
    blocks::Extrinsics,
    dynamic,
    ext::scale_value::{At, Value, ValueDef},
    storage,
};

// These limits are declared by deepx-node/runtime/testnet/src/lib.rs
const TIME_RANGE_MS: u64 = 3_600_000;
const WINDOW_SIZE: usize = 100;

pub(super) struct NonceEvidence {
    chain_time_ms: u64,
    used: Vec<u64>,
    unavailable: BTreeSet<u64>,
    quota: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct NoncePair {
    pub(super) front: u64,
    pub(super) back: u64,
}

impl NonceEvidence {
    pub(super) fn require_available(&self, nonce: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.chain_time_ms > 0 && self.quota > 1 && self.quota < u32::MAX,
            "placement quota is unavailable"
        );
        anyhow::ensure!(
            self.used.len() <= WINDOW_SIZE && self.used.windows(2).all(|w| w[0] < w[1]),
            "invalid timestamp nonce window"
        );
        anyhow::ensure!(
            nonce > self.chain_time_ms.saturating_sub(TIME_RANGE_MS)
                && nonce < self.chain_time_ms.saturating_add(TIME_RANGE_MS)
                && (self.used.len() < WINDOW_SIZE || nonce > self.used[0])
                && !self.used.contains(&nonce)
                && !self.unavailable.contains(&nonce),
            "signed placement nonce is no longer available"
        );
        Ok(())
    }

    pub(super) fn allocate_exit(&self, after: u64) -> anyhow::Result<u64> {
        anyhow::ensure!(
            self.chain_time_ms > 0 && self.quota > 1 && self.quota < u32::MAX,
            "exit placement quota is unavailable"
        );
        anyhow::ensure!(
            self.used.len() <= WINDOW_SIZE && self.used.windows(2).all(|w| w[0] < w[1]),
            "invalid timestamp nonce window"
        );
        let mut nonce = after
            .checked_add(1)
            .context("exit nonce overflow")?
            .max(self.chain_time_ms);
        if self.used.len() == WINDOW_SIZE {
            nonce = nonce.max(
                self.used[0]
                    .checked_add(1)
                    .context("nonce window exhausted")?,
            );
        }
        while self.used.contains(&nonce) || self.unavailable.contains(&nonce) {
            nonce = nonce.checked_add(1).context("exit nonce exhausted")?;
        }
        anyhow::ensure!(
            nonce < self.chain_time_ms.saturating_add(TIME_RANGE_MS),
            "exit nonce is outside the valid time range"
        );
        Ok(nonce)
    }

    pub(super) fn allocate(&self, target: u64) -> anyhow::Result<NoncePair> {
        anyhow::ensure!(self.chain_time_ms > 0, "chain timestamp must be positive");
        anyhow::ensure!(
            self.used.len() <= WINDOW_SIZE,
            "invalid timestamp nonce window length"
        );
        anyhow::ensure!(
            self.used.windows(2).all(|w| w[0] < w[1]),
            "unordered timestamp nonce window"
        );
        anyhow::ensure!(
            self.quota > 2 && self.quota < u32::MAX,
            "two placement quota units are unavailable"
        );

        // Time-range endpoints are excluded by check_time_nonce_range
        let mut lower = self
            .chain_time_ms
            .saturating_sub(TIME_RANGE_MS)
            .saturating_add(1);
        let upper = self
            .chain_time_ms
            .saturating_add(TIME_RANGE_MS)
            .saturating_sub(1);
        if self.used.len() == WINDOW_SIZE {
            lower = lower.max(
                self.used[0]
                    .checked_add(1)
                    .context("timestamp nonce window exhausted")?,
            );
        }
        let mut front = target
            .checked_sub(1)
            .context("target nonce has no predecessor")?
            .min(upper);
        while front >= lower && (self.used.contains(&front) || self.unavailable.contains(&front)) {
            front = front.checked_sub(1).context("front nonce exhausted")?;
        }
        anyhow::ensure!(
            front >= lower && front < target,
            "no valid nonce before target"
        );
        let mut back = target
            .checked_add(1)
            .context("target nonce has no successor")?
            .max(lower);
        while back <= upper && (self.used.contains(&back) || self.unavailable.contains(&back)) {
            back = back.checked_add(1).context("back nonce exhausted")?;
        }
        anyhow::ensure!(
            back <= upper && back > target,
            "no valid nonce after target"
        );
        Ok(NoncePair { front, back })
    }
}

pub(super) async fn observe(
    rpc: &DeepXRpcClient,
    metadata: &Metadata,
    head: &str,
    account: [u8; 20],
    pending: &[String],
) -> anyhow::Result<NonceEvidence> {
    let chain_time_ms = query_timestamp_now_at_block(rpc, super::parse_hash(head)?).await?;
    let address = dynamic::storage(
        "System",
        "Account",
        vec![dynamic::Value::from_bytes(account)],
    );
    let key = format!(
        "0x{}",
        hex::encode(storage::get_address_bytes(&address, metadata)?)
    );
    let encoded = rpc
        .get_storage(&key, head)
        .await?
        .context("ordering account does not exist")?;
    let bytes = hex::decode(
        encoded
            .strip_prefix("0x")
            .context("invalid account storage prefix")?,
    )?;
    let mut remaining = &bytes[..];
    let value_type = metadata
        .pallet_by_name("System")
        .context("missing System pallet")?
        .storage()
        .context("missing System storage")?
        .entry_by_name("Account")
        .context("missing System.Account")?
        .entry_type()
        .value_ty();
    let account_value = subxt_core::ext::scale_value::scale::decode_as_type(
        &mut remaining,
        value_type,
        metadata.types(),
    )?;
    anyhow::ensure!(remaining.is_empty(), "trailing account storage bytes");
    let (used, quota) = decode_window(&account_value)?;
    let mut unavailable = BTreeSet::new();

    // Include every pending operation for this signer, regardless of market or call
    for encoded in pending {
        let bytes = hex::decode(
            encoded
                .strip_prefix("0x")
                .context("invalid pending prefix")?,
        )?;
        let extrinsics =
            Extrinsics::<DeepXRuntimeConfig>::decode_from(vec![bytes], metadata.clone())?;
        let details = extrinsics
            .iter()
            .next()
            .context("missing pending extrinsic")?;
        if details.address_bytes() == Some(&account[..]) {
            let nonce = details
                .transaction_extensions()
                .and_then(|extensions| extensions.nonce())
                .context("pending account operation lacks a nonce")?;
            unavailable.insert(nonce);
        }
    }

    Ok(NonceEvidence {
        chain_time_ms,
        used,
        unavailable,
        quota,
    })
}

fn decode_window(account: &Value<u32>) -> anyhow::Result<(Vec<u64>, u32)> {
    let quota = account
        .at("quota")
        .and_then(Value::as_u128)
        .and_then(|value| u32::try_from(value).ok())
        .context("invalid account quota")?;
    let window = account
        .at("time_nonce")
        .context("missing timestamp nonce window")?;
    let nonces = window.at(0).context("missing timestamp nonce values")?;
    let ValueDef::Composite(nonces) = &nonces.value else {
        anyhow::bail!("invalid timestamp nonce window");
    };
    let nonces = nonces
        .values()
        .map(|value| {
            value
                .as_u128()
                .and_then(|value| u64::try_from(value).ok())
                .context("invalid timestamp nonce")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        nonces.len() <= WINDOW_SIZE,
        "invalid timestamp nonce window length"
    );
    anyhow::ensure!(
        nonces.windows(2).all(|w| w[0] < w[1]),
        "unordered timestamp nonce window"
    );
    Ok((nonces, quota))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn evidence() -> NonceEvidence {
        NonceEvidence {
            chain_time_ms: 10_000_000,
            used: vec![],
            unavailable: BTreeSet::new(),
            quota: 100,
        }
    }

    #[rstest]
    fn selects_nonces_on_opposite_sides_of_target() {
        assert_eq!(
            evidence().allocate(10_000_000).unwrap(),
            NoncePair {
                front: 9_999_999,
                back: 10_000_001
            }
        );
    }

    #[rstest]
    fn exit_allocation_survives_an_expired_target_and_skips_collisions() {
        let mut state = evidence();
        state.used = vec![10_000_000];
        state.unavailable.insert(10_000_001);
        assert_eq!(state.allocate_exit(1).unwrap(), 10_000_002);
        assert!(state.allocate_exit(13_599_999).is_err());
        state.quota = 2;
        assert_eq!(state.allocate_exit(1).unwrap(), 10_000_002);
        state.quota = 1;
        assert!(state.allocate_exit(1).is_err());
    }

    #[rstest]
    fn skips_used_and_pending_nonces_in_both_directions() {
        let mut state = evidence();
        state.used = vec![9_999_998, 10_000_002];
        state.unavailable.extend([9_999_999, 10_000_001]);
        assert_eq!(
            state.allocate(10_000_000).unwrap(),
            NoncePair {
                front: 9_999_997,
                back: 10_000_003
            }
        );
    }

    #[rstest]
    fn respects_full_window_floor_and_allows_out_of_order_below_maximum() {
        let mut state = evidence();
        state.used = (9_999_900..10_000_000).collect();
        assert!(state.allocate(9_999_901).is_err());
        assert_eq!(state.allocate(10_000_001).unwrap().front, 10_000_000);
        state.used = vec![10_000_100];
        assert_eq!(state.allocate(10_000_000).unwrap().front, 9_999_999);
    }

    #[rstest]
    #[case(6_400_000)]
    #[case(6_400_001)]
    #[case(13_599_999)]
    #[case(13_600_000)]
    #[case(0)]
    #[case(u64::MAX)]
    fn rejects_targets_without_room_for_both_legs(#[case] target: u64) {
        assert!(evidence().allocate(target).is_err());
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(u32::MAX)]
    fn rejects_unusable_account_quota(#[case] quota: u32) {
        let mut state = evidence();
        state.quota = quota;
        assert!(state.allocate(10_000_000).is_err());
    }

    #[rstest]
    fn rejects_unordered_or_oversized_windows() {
        let mut state = evidence();
        state.used = vec![2, 1];
        assert!(state.allocate(10_000_000).is_err());
        state.used = (0..101).collect();
        assert!(state.allocate(10_000_000).is_err());
    }

    #[test]
    fn signed_nonce_is_rechecked_without_requiring_it_to_equal_current_time() {
        let mut state = evidence();
        assert!(state.require_available(9_999_999).is_ok());
        state.used.push(9_999_999);
        assert!(state.require_available(9_999_999).is_err());
        state.used.clear();
        state.unavailable.insert(9_999_999);
        assert!(state.require_available(9_999_999).is_err());
        state.unavailable.clear();
        assert!(state.require_available(6_400_000).is_err());
        assert!(state.require_available(13_600_000).is_err());
        state.quota = 1;
        assert!(state.require_available(9_999_999).is_err());
    }
}
