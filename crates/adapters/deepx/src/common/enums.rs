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

//! DeepX environment and product selectors.

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// DeepX deployment environment.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum DeepXEnvironment {
    /// Production environment, currently unsupported.
    Mainnet,
    /// Public DeepX testnet environment.
    #[default]
    Testnet,
    /// Unrecognized environment retained for forward-compatible diagnostics.
    Unknown(String),
}

impl DeepXEnvironment {
    #[must_use]
    pub const fn is_testnet(&self) -> bool {
        matches!(self, Self::Testnet)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
            Self::Unknown(value) => value,
        }
    }
}

impl From<String> for DeepXEnvironment {
    fn from(value: String) -> Self {
        match value.as_str() {
            "mainnet" => Self::Mainnet,
            "testnet" => Self::Testnet,
            _ => Self::Unknown(value),
        }
    }
}

impl Display for DeepXEnvironment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for DeepXEnvironment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DeepXEnvironment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::from)
    }
}

/// DeepX market product family.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DeepXProductType {
    /// Spot market.
    Spot,
    /// Perpetual futures market.
    Perpetual,
    /// Unrecognized product retained for forward-compatible diagnostics.
    Unknown(String),
}

impl DeepXProductType {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Spot => "spot",
            Self::Perpetual => "perp",
            Self::Unknown(value) => value,
        }
    }
}

impl From<String> for DeepXProductType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "spot" => Self::Spot,
            "perp" => Self::Perpetual,
            _ => Self::Unknown(value),
        }
    }
}

impl Display for DeepXProductType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for DeepXProductType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DeepXProductType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::from)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("mainnet", DeepXEnvironment::Mainnet)]
    #[case("testnet", DeepXEnvironment::Testnet)]
    #[case("staging", DeepXEnvironment::Unknown("staging".to_string()))]
    fn environment_round_trips_unknown_values(
        #[case] raw: &str,
        #[case] expected: DeepXEnvironment,
    ) {
        let parsed: DeepXEnvironment = serde_json::from_str(&format!("\"{raw}\"")).unwrap();

        assert_eq!(parsed, expected);
        assert_eq!(
            serde_json::to_string(&parsed).unwrap(),
            format!("\"{raw}\"")
        );
    }

    #[rstest]
    #[case("spot", DeepXProductType::Spot)]
    #[case("perp", DeepXProductType::Perpetual)]
    #[case("option", DeepXProductType::Unknown("option".to_string()))]
    fn product_round_trips_unknown_values(#[case] raw: &str, #[case] expected: DeepXProductType) {
        let parsed: DeepXProductType = serde_json::from_str(&format!("\"{raw}\"")).unwrap();

        assert_eq!(parsed, expected);
        assert_eq!(
            serde_json::to_string(&parsed).unwrap(),
            format!("\"{raw}\"")
        );
    }
}
