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

use std::{fmt::Display, str::FromStr};

use serde::{Deserialize, Serialize};

pub const DEEPX_CLOID_MIN: u64 = 1 << 31;
pub const DEEPX_CLOID_MAX: u64 = u32::MAX as u64;

/// A DeepX client order ID which becomes the venue order ID when accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DeepXCloid(u32);

impl DeepXCloid {
    /// Creates a CLOID after validating the venue's reserved range.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is outside `2^31..=2^32 - 1`.
    pub fn new(value: u64) -> Result<Self, String> {
        let value = u32::try_from(value)
            .map_err(|_| format!("DeepX CLOID {value} exceeds {DEEPX_CLOID_MAX}"))?;
        if u64::from(value) < DEEPX_CLOID_MIN {
            return Err(format!("DeepX CLOID {value} is below {DEEPX_CLOID_MIN}"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl Display for DeepXCloid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Business-level lifecycle of a submitted DeepX extrinsic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeepXExecutionState {
    Submitting,
    Accepted,
    Executed,
    Finalized,
    Failed,
    NotIncluded,
    ActionRequired,
}

impl FromStr for DeepXExecutionState {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "submitting" => Ok(Self::Submitting),
            "accepted" => Ok(Self::Accepted),
            "executed" => Ok(Self::Executed),
            "finalized" => Ok(Self::Finalized),
            "failed" => Ok(Self::Failed),
            "not_included" => Ok(Self::NotIncluded),
            "action_required" => Ok(Self::ActionRequired),
            _ => Err(format!("Unknown DeepX execution state: {value}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(DEEPX_CLOID_MIN)]
    #[case(DEEPX_CLOID_MAX)]
    fn accepts_cloid_boundaries(#[case] value: u64) {
        assert_eq!(DeepXCloid::new(value).unwrap().value(), value as u32);
    }

    #[rstest]
    #[case(DEEPX_CLOID_MIN - 1)]
    #[case(DEEPX_CLOID_MAX + 1)]
    fn rejects_cloid_outside_reserved_range(#[case] value: u64) {
        assert!(DeepXCloid::new(value).is_err());
    }

    #[rstest]
    #[case("submitting", DeepXExecutionState::Submitting)]
    #[case("accepted", DeepXExecutionState::Accepted)]
    #[case("executed", DeepXExecutionState::Executed)]
    #[case("finalized", DeepXExecutionState::Finalized)]
    #[case("failed", DeepXExecutionState::Failed)]
    #[case("not_included", DeepXExecutionState::NotIncluded)]
    #[case("action_required", DeepXExecutionState::ActionRequired)]
    fn parses_execution_state(#[case] value: &str, #[case] expected: DeepXExecutionState) {
        assert_eq!(DeepXExecutionState::from_str(value).unwrap(), expected);
    }
}
