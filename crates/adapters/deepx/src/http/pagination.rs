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

//! Defensive state for DeepX cursor pagination.

use std::collections::HashSet;

use super::{DeepXHttpError, Result};

/// Decision produced after validating one cursor-paginated response page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PaginationDecision {
    /// Pagination is complete because the response omitted a non-empty cursor.
    Complete,
    /// Fetch another page using this cursor.
    Continue(String),
}

/// Tracks cursor progress and a local page budget for one pagination operation.
#[derive(Clone, Debug)]
pub struct CursorPagination {
    max_pages: usize,
    pages_observed: usize,
    seen_cursors: HashSet<String>,
}

impl CursorPagination {
    /// Creates pagination state with a strict local page limit.
    ///
    /// # Errors
    ///
    /// Returns [`DeepXHttpError::InvalidPaginationLimit`] when `max_pages` is zero.
    pub fn new(max_pages: usize) -> Result<Self> {
        if max_pages == 0 {
            return Err(DeepXHttpError::InvalidPaginationLimit);
        }

        Ok(Self {
            max_pages,
            pages_observed: 0,
            seen_cursors: HashSet::new(),
        })
    }

    /// Validates one response page and determines whether pagination should continue.
    ///
    /// Cursor direction, boundary inclusion, and row deduplication remain endpoint-specific.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the page budget is exceeded, an empty page advertises another
    /// page, or the response repeats a previously observed cursor.
    pub fn observe_page(
        &mut self,
        item_count: usize,
        next_cursor: Option<&str>,
    ) -> Result<PaginationDecision> {
        self.pages_observed += 1;

        let Some(cursor) = next_cursor.filter(|cursor| !cursor.is_empty()) else {
            return Ok(PaginationDecision::Complete);
        };
        if item_count == 0 {
            return Err(DeepXHttpError::PaginationNoProgress {
                cursor: cursor.to_string(),
            });
        }
        if !self.seen_cursors.insert(cursor.to_string()) {
            return Err(DeepXHttpError::RepeatedPaginationCursor {
                cursor: cursor.to_string(),
            });
        }
        if self.pages_observed == self.max_pages {
            return Err(DeepXHttpError::PaginationLimitExceeded {
                max_pages: self.max_pages,
            });
        }

        Ok(PaginationDecision::Continue(cursor.to_string()))
    }

    /// Returns the number of response pages observed so far.
    #[must_use]
    pub const fn pages_observed(&self) -> usize {
        self.pages_observed
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(None)]
    #[case(Some(""))]
    fn completes_without_non_empty_cursor(#[case] cursor: Option<&str>) {
        let mut pagination = CursorPagination::new(2).unwrap();

        assert_eq!(
            pagination.observe_page(3, cursor).unwrap(),
            PaginationDecision::Complete,
        );
        assert_eq!(pagination.pages_observed(), 1);
    }

    #[rstest]
    fn continues_with_new_cursor() {
        let mut pagination = CursorPagination::new(2).unwrap();

        assert_eq!(
            pagination.observe_page(3, Some("next-1")).unwrap(),
            PaginationDecision::Continue("next-1".to_string()),
        );
    }

    #[rstest]
    fn rejects_zero_page_limit() {
        assert!(matches!(
            CursorPagination::new(0),
            Err(DeepXHttpError::InvalidPaginationLimit),
        ));
    }

    #[rstest]
    fn rejects_empty_page_with_cursor() {
        let mut pagination = CursorPagination::new(2).unwrap();

        assert!(matches!(
            pagination.observe_page(0, Some("next-1")),
            Err(DeepXHttpError::PaginationNoProgress { cursor }) if cursor == "next-1",
        ));
    }

    #[rstest]
    fn rejects_repeated_cursor() {
        let mut pagination = CursorPagination::new(3).unwrap();
        pagination.observe_page(2, Some("next-1")).unwrap();

        assert!(matches!(
            pagination.observe_page(2, Some("next-1")),
            Err(DeepXHttpError::RepeatedPaginationCursor { cursor }) if cursor == "next-1",
        ));
    }

    #[rstest]
    fn rejects_page_beyond_limit() {
        let mut pagination = CursorPagination::new(1).unwrap();

        assert!(matches!(
            pagination.observe_page(2, Some("next-1")),
            Err(DeepXHttpError::PaginationLimitExceeded { max_pages: 1 }),
        ));
        assert_eq!(pagination.pages_observed(), 1);
    }
}
