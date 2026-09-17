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

//! Address-scoped, read-only DeepX account subscriptions.

use std::{collections::VecDeque, num::NonZeroUsize, time::Duration};

use nautilus_core::UUID4;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::http::{DeepXSubaccountBalances, client::validate_subaccount_balances};

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
        validate_address(address)?;
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DeepXWsAccountChannel {
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

/// Schema-validated frames for one DeepX user-balances subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeepXWsAccountFrame {
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

/// Opaque proof that one account subscription was acknowledged on a specific connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeepXWsConfirmedAccountSubscription {
    owner_id: UUID4,
    subaccount: [u8; 20],
}

impl DeepXWsConfirmedAccountSubscription {
    /// Returns the AccountId20 subaccount bound to this subscription.
    #[must_use]
    pub const fn subaccount(self) -> [u8; 20] {
        self.subaccount
    }
}

/// A balance frame admitted by the connection which owns its acknowledged subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsConfirmedBalancesFrame {
    subscription: DeepXWsConfirmedAccountSubscription,
    balances: DeepXSubaccountBalances,
    timestamp: u64,
}

impl DeepXWsConfirmedBalancesFrame {
    /// Returns the connection-owned subscription which admitted this frame.
    #[must_use]
    pub const fn subscription(&self) -> DeepXWsConfirmedAccountSubscription {
        self.subscription
    }

    /// Returns the exact validated account balances.
    #[must_use]
    pub const fn balances(&self) -> &DeepXSubaccountBalances {
        &self.balances
    }

    /// Returns the venue update timestamp in Unix milliseconds.
    #[must_use]
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl DeepXWsAccountFrame {
    /// Parses one account-channel frame and validates its market and balance payload.
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
                    validate_address(subscription.address())?;
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
                validate_address(&requested)?;
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

/// Single-owner account connection with acknowledgement-gated balance delivery.
#[derive(Debug)]
pub struct DeepXWsAccountConnection {
    owner_id: UUID4,
    transport: super::transport::DeepXWsReadConnection,
    confirmed_subaccount: Option<String>,
    confirmed_subscription: Option<DeepXWsConfirmedAccountSubscription>,
    buffered: VecDeque<DeepXWsAccountFrame>,
    buffer_capacity: NonZeroUsize,
    timeout: Duration,
    poisoned: bool,
}

impl DeepXWsAccountConnection {
    /// Opens one credential-free account-data connection.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, upgrade failure, or connection timeout.
    pub async fn connect(
        network: &crate::config::DeepXNetworkConfig,
        proxy_url: Option<&str>,
        timeout: Duration,
        buffer_capacity: NonZeroUsize,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            owner_id: UUID4::new(),
            transport: super::transport::DeepXWsReadConnection::connect(
                network, proxy_url, timeout,
            )
            .await?,
            confirmed_subaccount: None,
            confirmed_subscription: None,
            buffered: VecDeque::new(),
            buffer_capacity,
            timeout,
            poisoned: false,
        })
    }

    /// Subscribes to one exact subaccount and waits for its matching acknowledgement.
    ///
    /// Data arriving before the acknowledgement is bounded and withheld until confirmation.
    /// Timeout, overflow, or conflicting evidence makes the connection terminal.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identity, conflicting reuse, send failure, or invalid response.
    pub async fn subscribe_user_balances(
        &mut self,
        subaccount: &str,
    ) -> anyhow::Result<DeepXWsConfirmedAccountSubscription> {
        anyhow::ensure!(!self.poisoned, "DeepX account connection is terminal");
        let request = DeepXWsUserBalancesRequest::new(subaccount)?;
        if let Some(confirmed) = self.confirmed_subaccount.as_deref() {
            anyhow::ensure!(
                confirmed == request.subaccount(),
                "DeepX account connection is already bound to another subaccount"
            );
            return self.confirmed_subscription.ok_or_else(|| {
                anyhow::anyhow!("DeepX account subscription proof is unexpectedly absent")
            });
        }
        let expected = request.subaccount().to_string();
        let result = tokio::time::timeout(
            self.timeout,
            self.subscribe_inner(&request, expected.as_str()),
        )
        .await;
        match result {
            Ok(Ok(())) => {
                let subscription = DeepXWsConfirmedAccountSubscription {
                    owner_id: self.owner_id,
                    subaccount: parse_address(&expected)?,
                };
                self.confirmed_subaccount = Some(expected);
                self.confirmed_subscription = Some(subscription);
                Ok(subscription)
            }
            Ok(Err(e)) => {
                self.poisoned = true;
                self.buffered.clear();
                Err(e)
            }
            Err(_) => {
                self.poisoned = true;
                self.buffered.clear();
                anyhow::bail!("DeepX account subscription acknowledgement timed out");
            }
        }
    }

    /// Returns whether a subscription proof is current for this open connection.
    #[must_use]
    pub fn is_current_subscription(
        &self,
        subscription: DeepXWsConfirmedAccountSubscription,
    ) -> bool {
        !self.poisoned
            && subscription.owner_id == self.owner_id
            && self.confirmed_subscription == Some(subscription)
    }

    async fn subscribe_inner(
        &mut self,
        request: &DeepXWsUserBalancesRequest,
        expected: &str,
    ) -> anyhow::Result<()> {
        self.transport.send_account_request(request).await?;
        loop {
            let frame = self.read_frame().await?;
            match &frame {
                DeepXWsAccountFrame::Subscribed { .. } => {
                    return validate_ack(&frame, expected);
                }
                DeepXWsAccountFrame::UserBalances { balances, .. } => {
                    anyhow::ensure!(
                        balances.address.eq_ignore_ascii_case(expected),
                        "DeepX account data does not match subscription intent"
                    );
                    anyhow::ensure!(
                        self.buffered.len() < self.buffer_capacity.get(),
                        "DeepX account pre-ack buffer overflow"
                    );
                    self.buffered.push_back(frame);
                }
                DeepXWsAccountFrame::Pong { .. } => {}
                DeepXWsAccountFrame::Error { code, message } => {
                    anyhow::bail!("DeepX account subscription failed ({code}): {message}")
                }
            }
        }
    }

    /// Returns confirmed account data or a heartbeat; foreign frames fail closed.
    ///
    /// # Errors
    ///
    /// Returns an error for an unconfirmed, terminal, malformed, or foreign frame.
    async fn next_frame(&mut self) -> anyhow::Result<DeepXWsAccountFrame> {
        anyhow::ensure!(!self.poisoned, "DeepX account connection is terminal");
        let expected = self
            .confirmed_subaccount
            .clone()
            .ok_or_else(|| anyhow::anyhow!("DeepX account subscription is not confirmed"))?;
        if let Some(frame) = self.buffered.pop_front() {
            return Ok(frame);
        }
        let result = tokio::time::timeout(self.timeout, self.read_frame()).await;
        let frame = match result {
            Ok(Ok(frame)) => frame,
            Ok(Err(e)) => {
                if !matches!(
                    e.downcast_ref::<super::DeepXWsError>(),
                    Some(super::DeepXWsError::ReceiveTimeout)
                ) {
                    self.poisoned = true;
                }
                return Err(e);
            }
            Err(_) => return Err(super::DeepXWsError::ReceiveTimeout.into()),
        };
        let valid = match &frame {
            DeepXWsAccountFrame::UserBalances { balances, .. } => {
                balances.address.eq_ignore_ascii_case(&expected)
            }
            DeepXWsAccountFrame::Pong { .. } => true,
            DeepXWsAccountFrame::Subscribed { .. } | DeepXWsAccountFrame::Error { .. } => false,
        };
        if !valid {
            self.poisoned = true;
            anyhow::bail!("unexpected or foreign DeepX account frame");
        }
        Ok(frame)
    }

    /// Returns the next balance snapshot admitted by a current acknowledged subscription.
    ///
    /// Application heartbeat frames are consumed internally. A proof from another or closed
    /// connection is rejected before reading the transport.
    ///
    /// # Errors
    ///
    /// Returns an error for stale subscription proof, terminal transport, or invalid account data.
    pub async fn next_balances(
        &mut self,
        subscription: DeepXWsConfirmedAccountSubscription,
    ) -> anyhow::Result<DeepXWsConfirmedBalancesFrame> {
        anyhow::ensure!(
            self.is_current_subscription(subscription),
            "DeepX account subscription proof is not current"
        );
        loop {
            match self.next_frame().await? {
                DeepXWsAccountFrame::UserBalances {
                    balances,
                    timestamp,
                    ..
                } => {
                    return Ok(DeepXWsConfirmedBalancesFrame {
                        subscription,
                        balances,
                        timestamp,
                    });
                }
                DeepXWsAccountFrame::Pong { .. } => {}
                DeepXWsAccountFrame::Subscribed { .. } | DeepXWsAccountFrame::Error { .. } => {
                    unreachable!("next_frame rejects acknowledgements and errors")
                }
            }
        }
    }

    async fn read_frame(&mut self) -> anyhow::Result<DeepXWsAccountFrame> {
        loop {
            match self.transport.receive().await? {
                Some(nautilus_network::transport::Message::Text(bytes)) => {
                    return DeepXWsAccountFrame::parse(std::str::from_utf8(&bytes)?);
                }
                Some(
                    nautilus_network::transport::Message::Ping(_)
                    | nautilus_network::transport::Message::Pong(_),
                ) => {}
                _ => anyhow::bail!(
                    "DeepX account connection closed or received unsupported binary data"
                ),
            }
        }
    }

    /// Closes the transport and discards all address-binding evidence.
    ///
    /// # Errors
    ///
    /// Returns an error if the bounded transport close fails.
    pub async fn close(&mut self) -> anyhow::Result<()> {
        self.poisoned = true;
        self.confirmed_subaccount = None;
        self.confirmed_subscription = None;
        self.buffered.clear();
        self.transport.close().await
    }
}

fn validate_ack(frame: &DeepXWsAccountFrame, expected: &str) -> anyhow::Result<()> {
    let DeepXWsAccountFrame::Subscribed {
        market,
        subscriptions,
        ..
    } = frame
    else {
        anyhow::bail!("DeepX account response is not a subscription acknowledgement");
    };
    anyhow::ensure!(
        market.kind == "all"
            && subscriptions.len() == 1
            && subscriptions[0].channel == DeepXWsAccountChannel::UserBalances
            && subscriptions[0].address.eq_ignore_ascii_case(expected),
        "DeepX account subscription acknowledgement conflicts with pending intent"
    );
    Ok(())
}

fn validate_address(address: &str) -> anyhow::Result<()> {
    parse_address(address).map(|_| ())
}

fn parse_address(address: &str) -> anyhow::Result<[u8; 20]> {
    anyhow::ensure!(
        address.len() == 42
            && address.starts_with("0x")
            && address.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit),
        "DeepX user-balances subscription requires a 20-byte hex subaccount address"
    );
    Ok(nautilus_core::hex::decode_array::<20>(&address[2..])?)
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        extract::ws::{Message, WebSocketUpgrade},
        routing::get,
    };
    use rstest::rstest;
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::*;

    const SUBACCOUNT: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
    const BALANCES_FRAME: &str =
        include_str!("../../test_data/websocket/testnet/user_balances.json");

    #[rstest]
    fn request_binds_documented_aggregate_market_channel_and_address() {
        let request = DeepXWsUserBalancesRequest::new(SUBACCOUNT).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&request.to_text().unwrap()).unwrap(),
            json!({
                "action": "subscribe",
                "market": {"type": "all"},
                "subscriptions": [{"channel": "user_balances", "address": SUBACCOUNT}],
                "options": {"compress": false}
            })
        );
        assert_eq!(request.subaccount(), SUBACCOUNT);
    }

    #[rstest]
    #[case("")]
    #[case("4ded31cb63949b52f9dfc9bcfade4eab7017eadc")]
    #[case("0x4ded31cb63949b52f9dfc9bcfade4eab7017ead")]
    #[case("0x4ded31cb63949b52f9dfc9bcfade4eab7017eadcz")]
    fn request_rejects_non_account_id20_addresses(#[case] address: &str) {
        assert!(DeepXWsUserBalancesRequest::new(address).is_err());
    }

    #[rstest]
    fn captured_balance_frame_decodes_exact_values() {
        let frame = DeepXWsAccountFrame::parse(BALANCES_FRAME).unwrap();
        let DeepXWsAccountFrame::UserBalances {
            market,
            balances,
            timestamp,
        } = frame
        else {
            panic!("expected user balances frame");
        };
        assert_eq!(market.kind, "all");
        assert_eq!(balances.address, SUBACCOUNT);
        assert_eq!(balances.assets.len(), 3);
        assert_eq!(balances.assets[0].symbol, "USDC");
        assert_eq!(balances.assets[0].balance, Decimal::new(999_783_267, 6));
        assert_eq!(timestamp, 1_789_451_784_924);
    }

    #[rstest]
    #[case(concat!(
        r#"{"type":"data","channel":"user_balances","market":{"type":"perp","id":3},"#,
        r#""data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","#,
        r#""assets":[]},"timestamp":1}"#,
    ))]
    #[case(r#"{"type":"data","channel":"trades","market":{"type":"all"},"data":{},"timestamp":1}"#)]
    #[case(concat!(
        r#"{"type":"data","channel":"user_balances","market":{"type":"all"},"#,
        r#""data":{"address":"invalid","assets":[]},"timestamp":1}"#,
    ))]
    #[case(concat!(
        r#"{"type":"data","channel":"user_balances","market":{"type":"all"},"#,
        r#""data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","#,
        r#""assets":[]},"timestamp":0}"#,
    ))]
    #[case(concat!(
        r#"{"type":"subscribed","market":{"type":"all"},"subscriptions":[{"#,
        r#""channel":"user_balances","address":"#,
        r#""0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","addressType":"wallet"}],"#,
        r#""message":"ok"}"#,
    ))]
    fn malformed_or_foreign_account_frames_are_rejected(#[case] text: &str) {
        assert!(DeepXWsAccountFrame::parse(text).is_err());
    }

    struct Server(tokio::task::JoinHandle<()>);

    impl Drop for Server {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[tokio::test]
    async fn connection_gates_balances_on_matching_ack(#[case] scenario: u8) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    let Some(Ok(Message::Text(request))) = socket.recv().await else {
                        panic!("expected subscription request");
                    };
                    let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                    assert_eq!(request["market"]["type"], "all");
                    assert_eq!(request["options"]["compress"], false);
                    let data = Message::Text(BALANCES_FRAME.into());
                    if matches!(scenario, 1..=3) {
                        socket.send(data.clone()).await.unwrap();
                    }
                    if scenario == 3 {
                        socket.send(data.clone()).await.unwrap();
                    }
                    let acknowledged = if scenario == 2 {
                        "0x1111111111111111111111111111111111111111"
                    } else {
                        SUBACCOUNT
                    };
                    let ack = json!({
                        "type": "subscribed",
                        "market": {"type": "all"},
                        "subscriptions": [{
                            "channel": "user_balances",
                            "address": acknowledged
                        }],
                        "message": "Successfully subscribed to 1 channels"
                    });
                    let _ = socket.send(Message::Text(ack.to_string().into())).await;
                    if scenario == 0 {
                        let _ = socket.send(data).await;
                    } else if scenario == 4 {
                        let foreign = BALANCES_FRAME
                            .replace(SUBACCOUNT, "0x1111111111111111111111111111111111111111");
                        let _ = socket.send(Message::Text(foreign.into())).await;
                    }
                    let _ = tokio::time::timeout(Duration::from_secs(2), socket.recv()).await;
                })
            }),
        );
        let _server = Server(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let mut connection = DeepXWsAccountConnection::connect(
            &network,
            None,
            Duration::from_secs(1),
            NonZeroUsize::new(1).unwrap(),
        )
        .await
        .unwrap();
        let result = connection.subscribe_user_balances(SUBACCOUNT).await;
        if matches!(scenario, 0 | 1) {
            let subscription = result.unwrap();
            assert_eq!(
                subscription.subaccount(),
                nautilus_core::hex::decode_array::<20>(&SUBACCOUNT[2..]).unwrap()
            );
            let frame = connection.next_balances(subscription).await.unwrap();
            assert_eq!(frame.subscription(), subscription);
            assert_eq!(frame.balances().address, SUBACCOUNT);
            assert_eq!(
                connection
                    .subscribe_user_balances(SUBACCOUNT)
                    .await
                    .unwrap(),
                subscription
            );
        } else if scenario == 4 {
            let subscription = result.unwrap();
            assert!(connection.next_balances(subscription).await.is_err());
            assert!(!connection.is_current_subscription(subscription));
            assert!(connection.next_balances(subscription).await.is_err());
        } else {
            assert!(result.is_err());
            assert!(
                connection
                    .subscribe_user_balances(SUBACCOUNT)
                    .await
                    .is_err()
            );
        }
        let subscription = connection.confirmed_subscription;
        connection.close().await.unwrap();
        if let Some(subscription) = subscription {
            assert!(!connection.is_current_subscription(subscription));
            assert!(connection.next_balances(subscription).await.is_err());
        }
    }
}
