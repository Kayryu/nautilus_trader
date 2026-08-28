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

use std::str::FromStr;

use serde::{Deserialize, Serialize};

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
