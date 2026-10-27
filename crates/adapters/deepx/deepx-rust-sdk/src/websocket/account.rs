// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Address-scoped DeepX account subscription protocol.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::rest::models::{DeepXSubaccountBalances, validate_subaccount_balances};

/// The aggregate market identity required by portfolio channels.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeepXWsAllMarket {
    /// Market kind; account consumers require this to equal `all`.
    #[serde(rename = "type")]
    pub kind: String,
}

/// One address-scoped user-balances subscription.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeepXWsUserBalancesSubscription {
    channel: DeepXWsAccountChannel,
    address: String,
}

impl DeepXWsUserBalancesSubscription {
    fn new(address: &str) -> anyhow::Result<Self> {
        parse_subaccount_address(address)?;
        Ok(Self {
            channel: DeepXWsAccountChannel::UserBalances,
            address: address.to_ascii_lowercase(),
        })
    }

    /// Returns the subscribed subaccount address.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }
}

/// Account data channels accepted by the user-balances protocol.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeepXWsAccountChannel {
    /// Address-scoped lending balances.
    UserBalances,
}

/// A documented user-balances subscription request for one exact subaccount.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsUserBalancesRequest {
    subscription: DeepXWsUserBalancesSubscription,
}

impl DeepXWsUserBalancesRequest {
    /// Creates a read-only user-balances subscription for an AccountId20 subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error unless `subaccount` is a `0x`-prefixed 20-byte hexadecimal address.
    pub fn new(subaccount: &str) -> anyhow::Result<Self> {
        Ok(Self {
            subscription: DeepXWsUserBalancesSubscription::new(subaccount)?,
        })
    }

    /// Returns the canonical lower-case subaccount address.
    #[must_use]
    pub fn subaccount(&self) -> &str {
        self.subscription.address()
    }

    /// Serializes the documented aggregate-market subscription with compression disabled.
    ///
    /// # Errors
    ///
    /// Returns an error if the validated request cannot be serialized.
    pub fn to_text(&self) -> anyhow::Result<String> {
        #[derive(Serialize)]
        struct Request<'a> {
            action: &'static str,
            market: DeepXWsAllMarket,
            subscriptions: [&'a DeepXWsUserBalancesSubscription; 1],
            options: Options,
        }
        #[derive(Serialize)]
        struct Options {
            compress: bool,
        }

        Ok(serde_json::to_string(&Request {
            action: "subscribe",
            market: DeepXWsAllMarket {
                kind: "all".to_string(),
            },
            subscriptions: [&self.subscription],
            options: Options { compress: false },
        })?)
    }
}

/// Parses a `0x`-prefixed AccountId20 address used by account subscriptions.
///
/// # Errors
///
/// Returns an error unless the address contains exactly 20 hexadecimal bytes.
pub fn parse_subaccount_address(address: &str) -> anyhow::Result<[u8; 20]> {
    anyhow::ensure!(
        address.len() == 42
            && address.starts_with("0x")
            && address.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit),
        "DeepX user-balances subscription requires a 20-byte hex subaccount address"
    );
    let mut bytes = [0; 20];
    hex::decode_to_slice(&address[2..], &mut bytes)?;
    Ok(bytes)
}

/// Wire-level frames for one DeepX user-balances subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeepXWsAccountWireFrame {
    /// The server accepted one or more account subscription descriptors.
    Subscribed {
        /// Aggregate market identity echoed by the server.
        market: DeepXWsAllMarket,
        /// Exact address-scoped subscriptions echoed by the server.
        subscriptions: Vec<DeepXWsUserBalancesSubscription>,
        /// Informational acknowledgement message.
        message: String,
    },
    /// A complete current lending-balance snapshot for one subaccount.
    UserBalances {
        /// Aggregate market identity carried by the update.
        market: DeepXWsAllMarket,
        /// Exact validated account balances.
        balances: DeepXSubaccountBalances,
        /// Venue update timestamp in Unix milliseconds.
        timestamp: u64,
    },
    /// Application heartbeat response; timestamp units are not inferred.
    Pong { timestamp: u64 },
    /// Server protocol error, not evidence of a trading outcome.
    Error { code: String, message: String },
}

impl DeepXWsAccountWireFrame {
    /// Parses an account-channel frame and validates its market and balance payload.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, unsupported, foreign-market, or invalid balance frames.
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct WireFrame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsAllMarket>,
            subscriptions: Option<Vec<DeepXWsUserBalancesSubscription>>,
            message: Option<String>,
            channel: Option<DeepXWsAccountChannel>,
            data: Option<Box<RawValue>>,
            timestamp: Option<u64>,
            code: Option<String>,
        }

        fn required<T>(value: Option<T>, name: &str) -> anyhow::Result<T> {
            value.ok_or_else(|| anyhow::anyhow!("DeepX account frame missing {name}"))
        }

        let wire: WireFrame = serde_json::from_str(text)?;
        let frame = match wire.kind.as_str() {
            "subscribed" => {
                let subscriptions = required(wire.subscriptions, "subscriptions")?;
                for subscription in &subscriptions {
                    parse_subaccount_address(subscription.address())?;
                }
                Self::Subscribed {
                    market: required(wire.market, "market")?,
                    subscriptions,
                    message: required(wire.message, "message")?,
                }
            }
            "data" => {
                anyhow::ensure!(
                    wire.channel == Some(DeepXWsAccountChannel::UserBalances),
                    "unsupported DeepX account data channel"
                );
                let raw = required(wire.data, "data")?;
                let balances: DeepXSubaccountBalances = serde_json::from_str(raw.get())?;
                let requested = balances.address.clone();
                parse_subaccount_address(&requested)?;
                validate_subaccount_balances(&balances, &requested)?;
                let timestamp = required(wire.timestamp, "timestamp")?;
                anyhow::ensure!(
                    timestamp > 0 && i64::try_from(timestamp).is_ok(),
                    "invalid DeepX account frame timestamp"
                );
                Self::UserBalances {
                    market: required(wire.market, "market")?,
                    balances,
                    timestamp,
                }
            }
            "pong" => Self::Pong {
                timestamp: required(wire.timestamp, "timestamp")?,
            },
            "error" => Self::Error {
                code: required(wire.code, "code")?,
                message: required(wire.message, "message")?,
            },
            _ => anyhow::bail!("unsupported DeepX account frame type"),
        };
        let market = match &frame {
            Self::Subscribed { market, .. } | Self::UserBalances { market, .. } => Some(market),
            Self::Pong { .. } | Self::Error { .. } => None,
        };
        anyhow::ensure!(
            market.is_none_or(|market| market.kind == "all"),
            "DeepX account frame is not aggregate-market scoped"
        );
        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::*;

    const ACCOUNT: &str = "0x1111111111111111111111111111111111111111";

    #[rstest]
    fn balance_frame_preserves_exact_amounts() {
        let text = json!({
            "type": "data",
            "channel": "user_balances",
            "market": {"type": "all"},
            "data": {
                "address": ACCOUNT,
                "assets": [{
                    "symbol": "USDC",
                    "decimals": 6,
                    "price": "1.0",
                    "balance": "999.783267",
                    "balanceUsd": "999.783267",
                    "balanceBorrowed": "0",
                    "balanceBorrowedUsd": "0",
                    "borrowInterest": "0",
                    "borrowInterestUsd": "0"
                }]
            },
            "timestamp": 1
        });

        let DeepXWsAccountWireFrame::UserBalances { balances, .. } =
            DeepXWsAccountWireFrame::parse(&text.to_string()).unwrap()
        else {
            panic!("expected user balances frame");
        };
        assert_eq!(balances.address, ACCOUNT);
        assert_eq!(balances.assets[0].balance, Decimal::new(999_783_267, 6));
    }

    #[rstest]
    #[case::foreign_market(json!({"type": "perp"}))]
    #[case::extra_market_field(json!({"type": "all", "id": 1}))]
    fn balance_frame_rejects_invalid_market(#[case] market: serde_json::Value) {
        let text = json!({
            "type": "data",
            "channel": "user_balances",
            "market": market,
            "data": {"address": ACCOUNT, "assets": []},
            "timestamp": 1
        });
        assert!(DeepXWsAccountWireFrame::parse(&text.to_string()).is_err());
    }

    #[rstest]
    #[case::invalid_address("invalid", 1)]
    #[case::zero_timestamp(ACCOUNT, 0)]
    #[case::oversized_timestamp(ACCOUNT, u64::MAX)]
    fn balance_frame_rejects_invalid_identity_or_timestamp(
        #[case] address: &str,
        #[case] timestamp: u64,
    ) {
        let text = json!({
            "type": "data",
            "channel": "user_balances",
            "market": {"type": "all"},
            "data": {"address": address, "assets": []},
            "timestamp": timestamp
        });
        assert!(DeepXWsAccountWireFrame::parse(&text.to_string()).is_err());
    }

    #[rstest]
    fn balance_frame_rejects_duplicate_assets() {
        let asset = json!({
            "symbol": "USDC",
            "decimals": 6,
            "price": "1",
            "balance": "1",
            "balanceUsd": "1",
            "balanceBorrowed": "0",
            "balanceBorrowedUsd": "0",
            "borrowInterest": "0",
            "borrowInterestUsd": "0"
        });
        let mut duplicate = asset.clone();
        duplicate["symbol"] = json!("usdc");
        let text = json!({
            "type": "data",
            "channel": "user_balances",
            "market": {"type": "all"},
            "data": {"address": ACCOUNT, "assets": [asset, duplicate]},
            "timestamp": 1
        });
        assert!(DeepXWsAccountWireFrame::parse(&text.to_string()).is_err());
    }
}
