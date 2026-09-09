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
        Self::new_with_cursor(max_pages, None)
    }

    /// Creates pagination state with a strict page limit and an optional initial cursor.
    ///
    /// The initial cursor is treated as already observed so a response which returns it unchanged
    /// fails before another request can repeat the same page.
    ///
    /// # Errors
    ///
    /// Returns an error when `max_pages` is zero or the initial cursor is empty.
    pub fn new_with_cursor(max_pages: usize, initial_cursor: Option<&str>) -> Result<Self> {
        if max_pages == 0 {
            return Err(DeepXHttpError::InvalidPaginationLimit);
        }
        if initial_cursor.is_some_and(str::is_empty) {
            return Err(DeepXHttpError::InvalidRequest(
                "pagination initial cursor must not be empty".to_string(),
            ));
        }

        let mut seen_cursors = HashSet::new();
        if let Some(cursor) = initial_cursor {
            seen_cursors.insert(cursor.to_string());
        }

        Ok(Self {
            max_pages,
            pages_observed: 0,
            seen_cursors,
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

    /// Validates one response envelope before advancing the cursor state.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the response claims another page without a usable cursor, or
    /// when the ordinary cursor progress invariants fail.
    pub fn observe_response_page(
        &mut self,
        endpoint: &'static str,
        item_count: usize,
        has_next: bool,
        next_cursor: Option<&str>,
    ) -> Result<PaginationDecision> {
        validate_cursor_page(endpoint, has_next, next_cursor)?;
        self.observe_page(item_count, has_next.then_some(next_cursor).flatten())
    }

    /// Returns the number of response pages observed so far.
    #[must_use]
    pub const fn pages_observed(&self) -> usize {
        self.pages_observed
    }
}

pub(crate) fn validate_cursor_page(
    endpoint: &'static str,
    has_next: bool,
    next_cursor: Option<&str>,
) -> Result<()> {
    if has_next && next_cursor.is_none_or(str::is_empty) {
        return Err(DeepXHttpError::MissingPaginationCursor { endpoint });
    }
    Ok(())
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
    fn rejects_empty_initial_cursor() {
        assert!(matches!(
            CursorPagination::new_with_cursor(2, Some("")),
            Err(DeepXHttpError::InvalidRequest(message))
                if message.contains("initial cursor"),
        ));
    }

    #[rstest]
    fn rejects_initial_cursor_repeated_by_first_response() {
        let mut pagination = CursorPagination::new_with_cursor(2, Some("next-1")).unwrap();

        assert!(matches!(
            pagination.observe_response_page("perp trades", 2, true, Some("next-1")),
            Err(DeepXHttpError::RepeatedPaginationCursor { cursor }) if cursor == "next-1",
        ));
        assert_eq!(pagination.pages_observed(), 1);
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

    #[rstest]
    #[case(None)]
    #[case(Some(""))]
    fn response_page_rejects_missing_continuation_cursor(#[case] cursor: Option<&str>) {
        let mut pagination = CursorPagination::new(2).unwrap();

        assert!(matches!(
            pagination.observe_response_page("perp trades", 1, true, cursor),
            Err(DeepXHttpError::MissingPaginationCursor {
                endpoint: "perp trades"
            }),
        ));
        assert_eq!(pagination.pages_observed(), 0);
    }

    #[rstest]
    fn response_page_ignores_unused_terminal_cursor() {
        let mut pagination = CursorPagination::new(2).unwrap();

        assert_eq!(
            pagination
                .observe_response_page("perp trades", 1, false, Some("unused"))
                .unwrap(),
            PaginationDecision::Complete,
        );
    }
}
