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

//! Fail-closed classification of DeepX transaction recovery evidence.

use thiserror::Error;

use super::{DeepXAbsenceEvidence, DeepXInclusionEvidence};

/// Errors raised when a missed-block recovery scan cannot be planned safely.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum DeepXRecoveryScanPlanError {
    /// The finalized head is behind the last completely scanned block.
    #[error(
        "DeepX finalized block {finalized_block_number} is behind last scanned block {last_scanned_block}"
    )]
    FinalizedHeadBehind {
        /// Last block covered by a prior complete canonical scan.
        last_scanned_block: u64,
        /// Current finalized block reported by the recovery endpoint.
        finalized_block_number: u64,
    },
    /// The configured scan batch cannot contain any blocks.
    #[error("DeepX recovery scan batch size must be greater than zero")]
    ZeroBatchSize,
}

/// Inclusive canonical block interval for one bounded recovery request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXRecoveryScanRange {
    first_block: u64,
    last_block: u64,
}

impl DeepXRecoveryScanRange {
    /// Returns the first block in the inclusive interval.
    #[must_use]
    pub const fn first_block(&self) -> u64 {
        self.first_block
    }

    /// Returns the last block in the inclusive interval.
    #[must_use]
    pub const fn last_block(&self) -> u64 {
        self.last_block
    }
}

/// Bounded plan for scanning canonical blocks missed since the last complete scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeepXMissedBlockScanPlan {
    /// The prior scan already reaches the current finalized head.
    UpToDate,
    /// Canonical blocks must be scanned through the finalized boundary.
    Scan(DeepXRecoveryScanRanges),
}

/// Iterator over contiguous, non-overlapping inclusive recovery scan intervals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXRecoveryScanRanges {
    next_block: Option<u64>,
    finalized_block_number: u64,
    max_blocks_per_range: u64,
}

/// Errors raised while collecting canonical evidence for a planned recovery scan.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum DeepXRecoveryScanCollectionError {
    /// Every planned range has already been collected.
    #[error("DeepX recovery scan collection is already complete")]
    CollectionComplete,
    /// A batch did not match the next planned inclusive range.
    #[error("DeepX recovery scan range {received:?} does not match expected range {expected:?}")]
    UnexpectedRange {
        /// Next range required by the scan plan.
        expected: DeepXRecoveryScanRange,
        /// Range associated with the received block evidence.
        received: DeepXRecoveryScanRange,
    },
    /// A batch did not contain exactly one item for every block in its range.
    #[error("DeepX recovery scan range {range:?} requires {expected} blocks, received {received}")]
    BlockCountMismatch {
        /// Range associated with the received block evidence.
        range: DeepXRecoveryScanRange,
        /// Exact number of blocks required by the range.
        expected: u64,
        /// Number of block evidence items received.
        received: usize,
    },
    /// Block evidence was not ordered contiguously within its declared range.
    #[error("DeepX recovery block at offset {offset} has number {received}, expected {expected}")]
    UnexpectedBlockNumber {
        /// Zero-based offset within the received batch.
        offset: usize,
        /// Block number required at the offset.
        expected: u64,
        /// Block number present at the offset.
        received: u64,
    },
    /// Not every range in the scan plan has been collected.
    #[error("DeepX recovery scan is incomplete; next required range is {next_range:?}")]
    Incomplete {
        /// Next range that must be collected.
        next_range: DeepXRecoveryScanRange,
    },
}

/// Single-owner collector for canonical evidence matching one missed-block scan plan.
#[derive(Debug)]
pub struct DeepXRecoveryScanCollector {
    remaining_ranges: DeepXRecoveryScanRanges,
    next_range: Option<DeepXRecoveryScanRange>,
    first_scanned_block: u64,
    finalized_block_number: u64,
    blocks: Vec<DeepXCanonicalBlockEvidence>,
}

impl DeepXRecoveryScanCollector {
    /// Creates a collector for a non-empty planned scan.
    #[must_use]
    pub fn new(mut ranges: DeepXRecoveryScanRanges) -> Option<Self> {
        let next_range = ranges.next()?;
        Some(Self {
            first_scanned_block: next_range.first_block,
            finalized_block_number: ranges.finalized_block_number,
            remaining_ranges: ranges,
            next_range: Some(next_range),
            blocks: Vec::new(),
        })
    }

    /// Returns the next planned range that must be collected.
    #[must_use]
    pub const fn next_range(&self) -> Option<DeepXRecoveryScanRange> {
        self.next_range
    }

    /// Adds complete canonical evidence for the next planned range.
    ///
    /// # Errors
    ///
    /// Returns an error when the range is out of order or its evidence is incomplete, excessive,
    /// or not ordered contiguously by block number.
    pub fn push_range(
        &mut self,
        range: DeepXRecoveryScanRange,
        blocks: Vec<DeepXCanonicalBlockEvidence>,
    ) -> Result<(), DeepXRecoveryScanCollectionError> {
        let Some(expected_range) = self.next_range else {
            return Err(DeepXRecoveryScanCollectionError::CollectionComplete);
        };
        if range != expected_range {
            return Err(DeepXRecoveryScanCollectionError::UnexpectedRange {
                expected: expected_range,
                received: range,
            });
        }

        let expected_count = range.last_block - range.first_block + 1;
        if u64::try_from(blocks.len()) != Ok(expected_count) {
            return Err(DeepXRecoveryScanCollectionError::BlockCountMismatch {
                range,
                expected: expected_count,
                received: blocks.len(),
            });
        }
        for (offset, block) in blocks.iter().enumerate() {
            let expected_number = range.first_block + offset as u64;
            if block.block_number != expected_number {
                return Err(DeepXRecoveryScanCollectionError::UnexpectedBlockNumber {
                    offset,
                    expected: expected_number,
                    received: block.block_number,
                });
            }
        }

        self.blocks.extend(blocks);
        self.next_range = self.remaining_ranges.next();
        Ok(())
    }

    /// Completes collection and constructs evidence for fail-closed classification.
    ///
    /// # Errors
    ///
    /// Returns an error when any planned scan range has not been collected.
    pub fn finish(
        self,
        finalized_block_hash: [u8; 32],
        submission_pool: DeepXSubmissionPoolEvidence,
    ) -> Result<DeepXRecoveryScan, DeepXRecoveryScanCollectionError> {
        if let Some(next_range) = self.next_range {
            return Err(DeepXRecoveryScanCollectionError::Incomplete { next_range });
        }

        Ok(DeepXRecoveryScan::new(
            self.first_scanned_block,
            self.finalized_block_number,
            finalized_block_hash,
            self.blocks,
            submission_pool,
        ))
    }
}

impl Iterator for DeepXRecoveryScanRanges {
    type Item = DeepXRecoveryScanRange;

    fn next(&mut self) -> Option<Self::Item> {
        let next_block = self.next_block?;
        if next_block > self.finalized_block_number {
            self.next_block = None;
            return None;
        }

        let first_block = next_block;
        let last_block = first_block
            .saturating_add(self.max_blocks_per_range - 1)
            .min(self.finalized_block_number);
        self.next_block = last_block.checked_add(1);
        Some(DeepXRecoveryScanRange {
            first_block,
            last_block,
        })
    }
}

/// Plans bounded canonical ranges after the last completely scanned block.
///
/// The plan performs no network I/O and does not prove that returned ranges were scanned. Callers
/// must collect and validate every planned block before constructing absence evidence.
///
/// # Errors
///
/// Returns an error when the finalized head moves behind the durable scan checkpoint or the batch
/// size is zero.
pub fn plan_missed_block_scan(
    last_scanned_block: u64,
    finalized_block_number: u64,
    max_blocks_per_range: u64,
) -> Result<DeepXMissedBlockScanPlan, DeepXRecoveryScanPlanError> {
    if max_blocks_per_range == 0 {
        return Err(DeepXRecoveryScanPlanError::ZeroBatchSize);
    }
    if finalized_block_number < last_scanned_block {
        return Err(DeepXRecoveryScanPlanError::FinalizedHeadBehind {
            last_scanned_block,
            finalized_block_number,
        });
    }
    if finalized_block_number == last_scanned_block {
        return Ok(DeepXMissedBlockScanPlan::UpToDate);
    }

    Ok(DeepXMissedBlockScanPlan::Scan(DeepXRecoveryScanRanges {
        next_block: Some(last_scanned_block + 1),
        finalized_block_number,
        max_blocks_per_range,
    }))
}

/// Authoritative submission-node pool evidence for one transaction hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXSubmissionPoolEvidence {
    /// The transaction is present in the submission node pool.
    Present,
    /// The submission node authoritatively reported the transaction absent.
    Absent,
    /// Pool membership could not be determined authoritatively.
    Unknown,
}

/// One canonical block inspected during a recovery scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeepXCanonicalBlockEvidence {
    block_number: u64,
    block_hash: [u8; 32],
    inclusion: Option<DeepXInclusionEvidence>,
}

impl DeepXCanonicalBlockEvidence {
    /// Creates evidence for one fully inspected canonical block.
    #[must_use]
    pub const fn new(
        block_number: u64,
        block_hash: [u8; 32],
        inclusion: Option<DeepXInclusionEvidence>,
    ) -> Self {
        Self {
            block_number,
            block_hash,
            inclusion,
        }
    }
}

/// Complete inputs collected by a bounded recovery scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXRecoveryScan {
    first_scanned_block: u64,
    finalized_block_number: u64,
    finalized_block_hash: [u8; 32],
    blocks: Vec<DeepXCanonicalBlockEvidence>,
    submission_pool: DeepXSubmissionPoolEvidence,
}

impl DeepXRecoveryScan {
    /// Creates recovery inputs without asserting that the evidence is complete or consistent.
    #[must_use]
    pub fn new(
        first_scanned_block: u64,
        finalized_block_number: u64,
        finalized_block_hash: [u8; 32],
        blocks: Vec<DeepXCanonicalBlockEvidence>,
        submission_pool: DeepXSubmissionPoolEvidence,
    ) -> Self {
        Self {
            first_scanned_block,
            finalized_block_number,
            finalized_block_hash,
            blocks,
            submission_pool,
        }
    }

    /// Classifies the scan only when all required evidence is complete and consistent.
    #[must_use]
    pub fn classify(&self) -> DeepXRecoveryDecision {
        let Some(expected_len) = self
            .finalized_block_number
            .checked_sub(self.first_scanned_block)
            .and_then(|distance| distance.checked_add(1))
            .and_then(|length| usize::try_from(length).ok())
        else {
            return DeepXRecoveryDecision::ActionRequired;
        };
        if self.blocks.len() != expected_len {
            return DeepXRecoveryDecision::ActionRequired;
        }

        let mut inclusion = None;
        for (offset, block) in self.blocks.iter().enumerate() {
            let Ok(offset) = u64::try_from(offset) else {
                return DeepXRecoveryDecision::ActionRequired;
            };
            let Some(expected_number) = self.first_scanned_block.checked_add(offset) else {
                return DeepXRecoveryDecision::ActionRequired;
            };
            if block.block_number != expected_number {
                return DeepXRecoveryDecision::ActionRequired;
            }
            if let Some(candidate) = block.inclusion
                && (candidate.block_number != block.block_number
                    || candidate.block_hash != block.block_hash
                    || inclusion.replace(candidate).is_some())
            {
                return DeepXRecoveryDecision::ActionRequired;
            }
        }

        let Some(finalized) = self.blocks.last() else {
            return DeepXRecoveryDecision::ActionRequired;
        };
        if finalized.block_number != self.finalized_block_number
            || finalized.block_hash != self.finalized_block_hash
        {
            return DeepXRecoveryDecision::ActionRequired;
        }
        if let Some(inclusion) = inclusion {
            return DeepXRecoveryDecision::FinalizedInclusion(inclusion);
        }

        match self.submission_pool {
            DeepXSubmissionPoolEvidence::Present => DeepXRecoveryDecision::PoolAccepted,
            DeepXSubmissionPoolEvidence::Absent => {
                let Ok(absence) = DeepXAbsenceEvidence::new(
                    self.first_scanned_block,
                    self.finalized_block_number,
                    self.finalized_block_hash,
                    true,
                    true,
                ) else {
                    return DeepXRecoveryDecision::ActionRequired;
                };
                DeepXRecoveryDecision::NotIncluded(absence)
            }
            DeepXSubmissionPoolEvidence::Unknown => DeepXRecoveryDecision::ActionRequired,
        }
    }
}

/// Fail-closed decision produced from canonical-chain and submission-pool evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXRecoveryDecision {
    /// The transaction is still present in the submission node pool.
    PoolAccepted,
    /// The transaction was included in the scanned canonical finalized range.
    FinalizedInclusion(DeepXInclusionEvidence),
    /// Complete canonical scanning and a pool check proved absence.
    NotIncluded(DeepXAbsenceEvidence),
    /// Evidence is incomplete or conflicting and automatic recovery must stop.
    ActionRequired,
}

/// Decision produced by checking a recorded best-block inclusion against canonical block evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeepXReorganizationDecision {
    /// The recorded inclusion still belongs to the canonical block at its height.
    Canonical,
    /// A different canonical block replaced the block containing the recorded inclusion.
    Reorganized(DeepXInclusionEvidence),
    /// Canonical evidence is missing or inconsistent and automatic recovery must stop.
    ActionRequired,
}

/// Classifies whether a recorded non-finalized inclusion was removed by a reorganization.
///
/// A reorganization is proven only by a different canonical block hash at the exact recorded
/// height. An unchanged block hash must also contain the exact recorded inclusion; otherwise the
/// evidence source is internally inconsistent.
#[must_use]
pub fn classify_reorganization(
    recorded_inclusion: DeepXInclusionEvidence,
    canonical_block: Option<DeepXCanonicalBlockEvidence>,
) -> DeepXReorganizationDecision {
    let Some(canonical_block) = canonical_block else {
        return DeepXReorganizationDecision::ActionRequired;
    };
    if canonical_block.block_number != recorded_inclusion.block_number {
        return DeepXReorganizationDecision::ActionRequired;
    }
    if canonical_block.block_hash != recorded_inclusion.block_hash {
        return DeepXReorganizationDecision::Reorganized(recorded_inclusion);
    }
    if canonical_block.inclusion == Some(recorded_inclusion) {
        DeepXReorganizationDecision::Canonical
    } else {
        DeepXReorganizationDecision::ActionRequired
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::transaction::DeepXInclusionOutcome;

    #[rstest]
    fn missed_block_scan_is_split_into_contiguous_bounded_ranges() {
        let DeepXMissedBlockScanPlan::Scan(ranges) = plan_missed_block_scan(39, 46, 3).unwrap()
        else {
            panic!("expected missed-block scan");
        };

        assert_eq!(
            ranges.collect::<Vec<_>>(),
            vec![
                DeepXRecoveryScanRange {
                    first_block: 40,
                    last_block: 42,
                },
                DeepXRecoveryScanRange {
                    first_block: 43,
                    last_block: 45,
                },
                DeepXRecoveryScanRange {
                    first_block: 46,
                    last_block: 46,
                },
            ],
        );
    }

    #[rstest]
    fn scan_checkpoint_at_finalized_head_is_up_to_date() {
        assert_eq!(
            plan_missed_block_scan(42, 42, 10),
            Ok(DeepXMissedBlockScanPlan::UpToDate),
        );
    }

    #[rstest]
    #[case::zero_batch(42, 43, 0, DeepXRecoveryScanPlanError::ZeroBatchSize)]
    #[case::finalized_behind(
        43,
        42,
        10,
        DeepXRecoveryScanPlanError::FinalizedHeadBehind {
            last_scanned_block: 43,
            finalized_block_number: 42,
        },
    )]
    fn invalid_scan_plan_is_rejected(
        #[case] last_scanned_block: u64,
        #[case] finalized_block_number: u64,
        #[case] max_blocks_per_range: u64,
        #[case] expected: DeepXRecoveryScanPlanError,
    ) {
        assert_eq!(
            plan_missed_block_scan(
                last_scanned_block,
                finalized_block_number,
                max_blocks_per_range,
            ),
            Err(expected),
        );
    }

    #[rstest]
    fn scan_plan_handles_maximum_finalized_block_without_wrapping() {
        let DeepXMissedBlockScanPlan::Scan(mut ranges) =
            plan_missed_block_scan(u64::MAX - 1, u64::MAX, 10).unwrap()
        else {
            panic!("expected missed-block scan");
        };

        assert_eq!(
            ranges.next(),
            Some(DeepXRecoveryScanRange {
                first_block: u64::MAX,
                last_block: u64::MAX,
            }),
        );
        assert_eq!(ranges.next(), None);
    }

    #[rstest]
    fn scan_collector_rejects_skipped_planned_range() {
        let DeepXMissedBlockScanPlan::Scan(ranges) = plan_missed_block_scan(39, 45, 3).unwrap()
        else {
            panic!("expected missed-block scan");
        };
        let mut collector = DeepXRecoveryScanCollector::new(ranges).unwrap();
        let skipped = DeepXRecoveryScanRange {
            first_block: 43,
            last_block: 45,
        };

        assert_eq!(
            collector.push_range(
                skipped,
                vec![block(43, None), block(44, None), block(45, None)],
            ),
            Err(DeepXRecoveryScanCollectionError::UnexpectedRange {
                expected: DeepXRecoveryScanRange {
                    first_block: 40,
                    last_block: 42,
                },
                received: skipped,
            }),
        );
    }

    #[rstest]
    fn scan_collector_rejects_incomplete_batch() {
        let DeepXMissedBlockScanPlan::Scan(ranges) = plan_missed_block_scan(39, 42, 3).unwrap()
        else {
            panic!("expected missed-block scan");
        };
        let mut collector = DeepXRecoveryScanCollector::new(ranges).unwrap();
        let range = DeepXRecoveryScanRange {
            first_block: 40,
            last_block: 42,
        };

        assert_eq!(
            collector.push_range(range, vec![block(40, None), block(41, None)]),
            Err(DeepXRecoveryScanCollectionError::BlockCountMismatch {
                range,
                expected: 3,
                received: 2,
            }),
        );
    }

    #[rstest]
    fn incomplete_scan_collection_cannot_release_evidence() {
        let DeepXMissedBlockScanPlan::Scan(ranges) = plan_missed_block_scan(39, 43, 2).unwrap()
        else {
            panic!("expected missed-block scan");
        };
        let mut collector = DeepXRecoveryScanCollector::new(ranges).unwrap();
        let first_range = collector.next_range().unwrap();
        collector
            .push_range(first_range, vec![block(40, None), block(41, None)])
            .unwrap();

        assert_eq!(
            collector.finish([43; 32], DeepXSubmissionPoolEvidence::Absent),
            Err(DeepXRecoveryScanCollectionError::Incomplete {
                next_range: DeepXRecoveryScanRange {
                    first_block: 42,
                    last_block: 43,
                },
            }),
        );
    }

    #[rstest]
    fn scan_collector_rejects_non_contiguous_block_evidence() {
        let DeepXMissedBlockScanPlan::Scan(ranges) = plan_missed_block_scan(39, 42, 3).unwrap()
        else {
            panic!("expected missed-block scan");
        };
        let mut collector = DeepXRecoveryScanCollector::new(ranges).unwrap();
        let range = collector.next_range().unwrap();

        assert_eq!(
            collector.push_range(
                range,
                vec![block(40, None), block(42, None), block(41, None)],
            ),
            Err(DeepXRecoveryScanCollectionError::UnexpectedBlockNumber {
                offset: 1,
                expected: 41,
                received: 42,
            }),
        );
    }

    #[rstest]
    fn scan_collector_rejects_batches_after_completion() {
        let DeepXMissedBlockScanPlan::Scan(ranges) = plan_missed_block_scan(39, 40, 1).unwrap()
        else {
            panic!("expected missed-block scan");
        };
        let mut collector = DeepXRecoveryScanCollector::new(ranges).unwrap();
        let range = collector.next_range().unwrap();
        collector.push_range(range, vec![block(40, None)]).unwrap();

        assert_eq!(
            collector.push_range(range, vec![block(40, None)]),
            Err(DeepXRecoveryScanCollectionError::CollectionComplete),
        );
    }

    #[rstest]
    fn complete_scan_collection_releases_classifiable_evidence() {
        let DeepXMissedBlockScanPlan::Scan(ranges) = plan_missed_block_scan(39, 43, 2).unwrap()
        else {
            panic!("expected missed-block scan");
        };
        let mut collector = DeepXRecoveryScanCollector::new(ranges).unwrap();
        collector
            .push_range(
                DeepXRecoveryScanRange {
                    first_block: 40,
                    last_block: 41,
                },
                vec![block(40, None), block(41, None)],
            )
            .unwrap();
        collector
            .push_range(
                DeepXRecoveryScanRange {
                    first_block: 42,
                    last_block: 43,
                },
                vec![block(42, None), block(43, None)],
            )
            .unwrap();

        let scan = collector
            .finish([43; 32], DeepXSubmissionPoolEvidence::Absent)
            .unwrap();
        assert!(matches!(
            scan.classify(),
            DeepXRecoveryDecision::NotIncluded(_),
        ));
    }

    fn block(
        number: u64,
        inclusion: Option<DeepXInclusionEvidence>,
    ) -> DeepXCanonicalBlockEvidence {
        DeepXCanonicalBlockEvidence::new(number, [number as u8; 32], inclusion)
    }

    fn inclusion(number: u64) -> DeepXInclusionEvidence {
        DeepXInclusionEvidence {
            block_hash: [number as u8; 32],
            block_number: number,
            extrinsic_index: 3,
            outcome: DeepXInclusionOutcome::Success,
        }
    }

    #[rstest]
    fn changed_canonical_hash_proves_reorganization() {
        let recorded = inclusion(41);
        let replacement = DeepXCanonicalBlockEvidence::new(41, [9; 32], None);

        assert_eq!(
            classify_reorganization(recorded, Some(replacement)),
            DeepXReorganizationDecision::Reorganized(recorded),
        );
    }

    #[rstest]
    fn exact_canonical_inclusion_is_unchanged() {
        let recorded = inclusion(41);
        let canonical = block(41, Some(recorded));

        assert_eq!(
            classify_reorganization(recorded, Some(canonical)),
            DeepXReorganizationDecision::Canonical,
        );
    }

    #[rstest]
    #[case::missing(None)]
    #[case::wrong_height(Some(block(42, None)))]
    #[case::same_block_missing_inclusion(Some(block(41, None)))]
    #[case::same_block_conflicting_inclusion(Some(block(41, Some(DeepXInclusionEvidence {
        extrinsic_index: 4,
        ..inclusion(41)
    }))))]
    fn incomplete_or_inconsistent_reorganization_evidence_requires_action(
        #[case] canonical_block: Option<DeepXCanonicalBlockEvidence>,
    ) {
        assert_eq!(
            classify_reorganization(inclusion(41), canonical_block),
            DeepXReorganizationDecision::ActionRequired,
        );
    }

    fn complete_scan(pool: DeepXSubmissionPoolEvidence) -> DeepXRecoveryScan {
        DeepXRecoveryScan::new(
            40,
            42,
            [42; 32],
            vec![block(40, None), block(41, None), block(42, None)],
            pool,
        )
    }

    #[rstest]
    fn complete_scan_and_pool_absence_produce_not_included() {
        let decision = complete_scan(DeepXSubmissionPoolEvidence::Absent).classify();
        let DeepXRecoveryDecision::NotIncluded(evidence) = decision else {
            panic!("expected not-included decision");
        };

        assert_eq!(evidence.first_scanned_block(), 40);
        assert_eq!(evidence.finalized_block_number(), 42);
        assert_eq!(evidence.finalized_block_hash(), [42; 32]);
    }

    #[rstest]
    fn canonical_inclusion_takes_precedence_over_pool_evidence() {
        let inclusion = DeepXInclusionEvidence {
            block_hash: [41; 32],
            block_number: 41,
            extrinsic_index: 3,
            outcome: DeepXInclusionOutcome::Success,
        };
        let scan = DeepXRecoveryScan::new(
            40,
            42,
            [42; 32],
            vec![block(40, None), block(41, Some(inclusion)), block(42, None)],
            DeepXSubmissionPoolEvidence::Unknown,
        );

        assert_eq!(
            scan.classify(),
            DeepXRecoveryDecision::FinalizedInclusion(inclusion),
        );
    }

    #[rstest]
    #[case::missing_block(vec![block(40, None), block(42, None)], [42; 32])]
    #[case::wrong_order(vec![block(41, None), block(40, None), block(42, None)], [42; 32])]
    #[case::wrong_finalized_hash(
        vec![block(40, None), block(41, None), block(42, None)],
        [9; 32],
    )]
    fn incomplete_or_noncanonical_scan_requires_action(
        #[case] blocks: Vec<DeepXCanonicalBlockEvidence>,
        #[case] finalized_hash: [u8; 32],
    ) {
        let scan = DeepXRecoveryScan::new(
            40,
            42,
            finalized_hash,
            blocks,
            DeepXSubmissionPoolEvidence::Absent,
        );

        assert_eq!(scan.classify(), DeepXRecoveryDecision::ActionRequired);
    }

    #[rstest]
    fn unknown_pool_membership_requires_action_when_chain_has_no_inclusion() {
        assert_eq!(
            complete_scan(DeepXSubmissionPoolEvidence::Unknown).classify(),
            DeepXRecoveryDecision::ActionRequired,
        );
    }

    #[rstest]
    fn present_pool_transaction_is_not_classified_as_absent() {
        assert_eq!(
            complete_scan(DeepXSubmissionPoolEvidence::Present).classify(),
            DeepXRecoveryDecision::PoolAccepted,
        );
    }

    #[rstest]
    fn conflicting_inclusions_require_action() {
        let inclusion_40 = DeepXInclusionEvidence {
            block_hash: [40; 32],
            block_number: 40,
            extrinsic_index: 1,
            outcome: DeepXInclusionOutcome::Success,
        };
        let inclusion_41 = DeepXInclusionEvidence {
            block_hash: [41; 32],
            block_number: 41,
            extrinsic_index: 2,
            outcome: DeepXInclusionOutcome::Failed,
        };
        let scan = DeepXRecoveryScan::new(
            40,
            42,
            [42; 32],
            vec![
                block(40, Some(inclusion_40)),
                block(41, Some(inclusion_41)),
                block(42, None),
            ],
            DeepXSubmissionPoolEvidence::Absent,
        );

        assert_eq!(scan.classify(), DeepXRecoveryDecision::ActionRequired);
    }
}
