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

//! Process-local timestamp nonce allocation for DeepX extrinsics.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Instant,
};

use thiserror::Error;

const TIMESTAMP_WINDOW_MS: u64 = 60 * 60 * 1_000;

#[derive(Debug, Default)]
struct NonceState {
    high_water_mark: Option<u64>,
    reserved: HashSet<u64>,
}

/// Errors emitted while allocating DeepX timestamp nonces.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DeepXNonceError {
    /// The calibrated chain time cannot be represented in milliseconds.
    #[error("estimated chain time overflows u64 milliseconds")]
    ChainTimeOverflow,
    /// An explicit nonce falls outside the runtime's accepted timestamp window.
    #[error("explicit timestamp nonce is outside the chain's one-hour window")]
    OutsideTimestampWindow,
    /// A nonce has already been reserved by this allocator.
    #[error("timestamp nonce {0} is already reserved by this client")]
    AlreadyReserved(u64),
    /// No greater timestamp nonce can be represented.
    #[error("timestamp nonce space is exhausted")]
    Exhausted,
    /// The allocator's synchronization state was poisoned.
    #[error("timestamp nonce allocator lock is poisoned")]
    LockPoisoned,
}

/// Estimates DeepX chain time from an authoritative `Timestamp.Now` sample.
#[derive(Clone, Copy, Debug)]
pub struct DeepXChainTimeCalibration {
    chain_time_ms: u64,
    calibrated_at: Instant,
}

impl DeepXChainTimeCalibration {
    /// Anchors a chain timestamp to the current monotonic clock.
    #[must_use]
    pub fn new(chain_time_ms: u64) -> Self {
        Self {
            chain_time_ms,
            calibrated_at: Instant::now(),
        }
    }

    /// Returns the estimated chain time without depending on the system wall clock.
    ///
    /// # Errors
    ///
    /// Returns an error when elapsed monotonic time would overflow `u64` milliseconds.
    pub fn estimated_chain_time_ms(&self) -> Result<u64, DeepXNonceError> {
        self.estimated_at(Instant::now())
    }

    fn estimated_at(&self, now: Instant) -> Result<u64, DeepXNonceError> {
        let elapsed_ms = now
            .checked_duration_since(self.calibrated_at)
            .unwrap_or_default()
            .as_millis();
        let elapsed_ms =
            u64::try_from(elapsed_ms).map_err(|_| DeepXNonceError::ChainTimeOverflow)?;
        self.chain_time_ms
            .checked_add(elapsed_ms)
            .ok_or(DeepXNonceError::ChainTimeOverflow)
    }
}

/// Allocates unique millisecond timestamp nonces for one DeepX execution client.
#[derive(Clone, Debug, Default)]
pub struct DeepXTimestampNonceAllocator {
    state: Arc<Mutex<NonceState>>,
}

impl DeepXTimestampNonceAllocator {
    /// Creates an empty timestamp nonce allocator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserves the next nonce at or above the estimated chain time.
    ///
    /// # Errors
    ///
    /// Returns an error when the nonce space is exhausted or the allocator lock is poisoned.
    pub fn next(&self, estimated_chain_time_ms: u64) -> Result<u64, DeepXNonceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXNonceError::LockPoisoned)?;
        let mut nonce = match state.high_water_mark {
            Some(high_water_mark) => estimated_chain_time_ms.max(
                high_water_mark
                    .checked_add(1)
                    .ok_or(DeepXNonceError::Exhausted)?,
            ),
            None => estimated_chain_time_ms,
        };

        while state.reserved.contains(&nonce) {
            nonce = nonce.checked_add(1).ok_or(DeepXNonceError::Exhausted)?;
        }

        state.reserved.insert(nonce);
        state.high_water_mark = Some(nonce);
        Ok(nonce)
    }

    /// Reserves a caller-selected nonce within one hour of the estimated chain time.
    ///
    /// # Errors
    ///
    /// Returns an error when the nonce is outside the accepted timestamp window, already reserved,
    /// or the allocator lock is poisoned.
    pub fn reserve(
        &self,
        nonce: u64,
        estimated_chain_time_ms: u64,
    ) -> Result<u64, DeepXNonceError> {
        if nonce.abs_diff(estimated_chain_time_ms) > TIMESTAMP_WINDOW_MS {
            return Err(DeepXNonceError::OutsideTimestampWindow);
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| DeepXNonceError::LockPoisoned)?;
        if !state.reserved.insert(nonce) {
            return Err(DeepXNonceError::AlreadyReserved(nonce));
        }

        state.high_water_mark = Some(
            state
                .high_water_mark
                .map_or(nonce, |current| current.max(nonce)),
        );
        Ok(nonce)
    }

    /// Releases a nonce after local extrinsic construction fails before submission.
    ///
    /// # Errors
    ///
    /// Returns an error when the allocator lock is poisoned.
    pub fn release(&self, nonce: u64) -> Result<(), DeepXNonceError> {
        self.state
            .lock()
            .map_err(|_| DeepXNonceError::LockPoisoned)?
            .reserved
            .remove(&nonce);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use rstest::rstest;

    use super::*;

    #[rstest]
    fn calibrated_chain_time_advances_with_monotonic_elapsed_time() {
        let calibration = DeepXChainTimeCalibration {
            chain_time_ms: 10_000,
            calibrated_at: Instant::now(),
        };

        assert_eq!(
            calibration
                .estimated_at(calibration.calibrated_at + Duration::from_millis(250))
                .unwrap(),
            10_250,
        );
    }

    #[rstest]
    fn calibrated_chain_time_does_not_move_backwards() {
        let calibration = DeepXChainTimeCalibration {
            chain_time_ms: 10_000,
            calibrated_at: Instant::now(),
        };
        let earlier = calibration
            .calibrated_at
            .checked_sub(Duration::from_millis(250))
            .unwrap();

        assert_eq!(calibration.estimated_at(earlier).unwrap(), 10_000);
    }

    #[rstest]
    fn calibrated_chain_time_rejects_overflow() {
        let calibration = DeepXChainTimeCalibration {
            chain_time_ms: u64::MAX,
            calibrated_at: Instant::now(),
        };

        assert_eq!(
            calibration.estimated_at(calibration.calibrated_at + Duration::from_millis(1)),
            Err(DeepXNonceError::ChainTimeOverflow),
        );
    }

    #[rstest]
    fn allocates_strictly_monotonic_nonces() {
        let allocator = DeepXTimestampNonceAllocator::new();

        assert_eq!(allocator.next(1_000_000).unwrap(), 1_000_000);
        assert_eq!(allocator.next(1_000_000).unwrap(), 1_000_001);
        assert_eq!(allocator.next(999_000).unwrap(), 1_000_002);
    }

    #[rstest]
    fn explicit_nonce_advances_high_water_mark() {
        let allocator = DeepXTimestampNonceAllocator::new();

        allocator.reserve(1_100, 1_000).unwrap();

        assert_eq!(allocator.next(1_000).unwrap(), 1_101);
    }

    #[rstest]
    fn concurrent_allocations_are_unique() {
        let allocator = Arc::new(DeepXTimestampNonceAllocator::new());
        let handles = (0..500)
            .map(|_| {
                let allocator = Arc::clone(&allocator);
                std::thread::spawn(move || allocator.next(50_000).unwrap())
            })
            .collect::<Vec<_>>();
        let mut nonces = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        nonces.sort_unstable();

        assert_eq!(nonces, (50_000..50_500).collect::<Vec<_>>());
    }

    #[rstest]
    fn explicit_nonce_is_reserved_until_released() {
        let allocator = DeepXTimestampNonceAllocator::new();

        assert_eq!(allocator.reserve(1_001, 1_000).unwrap(), 1_001);
        assert_eq!(
            allocator.reserve(1_001, 1_000),
            Err(DeepXNonceError::AlreadyReserved(1_001)),
        );

        allocator.release(1_001).unwrap();
        assert_eq!(allocator.reserve(1_001, 1_000).unwrap(), 1_001);
    }

    #[rstest]
    #[case(10_000_000 - TIMESTAMP_WINDOW_MS - 1)]
    #[case(10_000_000 + TIMESTAMP_WINDOW_MS + 1)]
    fn rejects_explicit_nonce_outside_timestamp_window(#[case] nonce: u64) {
        let allocator = DeepXTimestampNonceAllocator::new();

        assert_eq!(
            allocator.reserve(nonce, 10_000_000),
            Err(DeepXNonceError::OutsideTimestampWindow),
        );
    }

    #[rstest]
    fn rejects_allocation_after_maximum_nonce() {
        let allocator = DeepXTimestampNonceAllocator::new();
        allocator.reserve(u64::MAX, u64::MAX).unwrap();

        assert_eq!(allocator.next(u64::MAX), Err(DeepXNonceError::Exhausted));
    }
}
