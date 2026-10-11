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
//  See the GNU Lesser General Public License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! One-minute perpetual candle protocol state, retaining updates until a newer bucket arrives.

use std::time::Duration;

use anyhow::{Result, ensure};
use nautilus_network::transport::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, value::RawValue};

use super::public::DeepXWsPerpMarket;
use crate::http::{DeepXPerpCandle, models::exact_decimal};

const ONE_MINUTE_MS: u64 = 60_000;

#[derive(Deserialize)]
struct CandlePage {
    pair: String,
    details: Vec<WireCandle>,
}

#[derive(Deserialize)]
struct WireCandle {
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    volume: Decimal,
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    high: Decimal,
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    low: Decimal,
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    open: Decimal,
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    close: Decimal,
    time: u64,
}

/// A raw candle superseded by a newer bucket, with an explicit missing-bucket indicator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXWsSupersededCandle {
    /// Last observed update for the preceding bucket on this connection.
    pub candle: DeepXPerpCandle,
    /// True when the next observed bucket is not the immediate successor.
    pub gap_after: bool,
}

/// Connection-local state for one market's one-minute trade candles.
#[derive(Debug)]
pub struct DeepXWsCandleStream {
    market_id: u16,
    pair: String,
    acknowledged: bool,
    pending: Option<DeepXPerpCandle>,
    last_timestamp_ms: Option<u64>,
    failed: bool,
}

impl DeepXWsCandleStream {
    /// Creates a stream bound to one perpetual market and its expected pair name.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid market identity.
    pub fn new(market_id: u16, pair: impl Into<String>) -> Result<Self> {
        let pair = pair.into();
        ensure!(
            market_id != 0 && !pair.is_empty(),
            "invalid DeepX candle market identity"
        );
        Ok(Self {
            market_id,
            pair,
            acknowledged: false,
            pending: None,
            last_timestamp_ms: None,
            failed: false,
        })
    }

    /// Serializes the documented read-only one-minute candle subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn subscription_text(&self) -> Result<String> {
        Ok(serde_json::json!({
            "action": "subscribe",
            "market": {"type": "perp", "id": self.market_id},
            "subscriptions": [{"channel": "candles", "time_frame": "1m"}],
            "options": {"compress": false},
        })
        .to_string())
    }

    /// Applies one server frame and returns a candle only when a newer bucket is observed.
    ///
    /// An acknowledgement containing only `time_frame` is provisional: the subsequent data
    /// frame must also bind the market, channel, interval, and pair. A stream is connection-local;
    /// after disconnect, create a new instance rather than carrying an incomplete bucket across.
    ///
    /// # Errors
    ///
    /// Returns an error for conflicting identity, malformed data, or out-of-order buckets.
    /// Any error makes this stream terminal; a new connection needs a new instance.
    pub fn apply(&mut self, text: &str) -> Result<Option<DeepXWsSupersededCandle>> {
        ensure!(!self.failed, "DeepX candle stream is terminal");
        let result = self.apply_inner(text);
        if result.is_err() {
            self.failed = true;
            self.pending = None;
        }
        result
    }

    fn apply_inner(&mut self, text: &str) -> Result<Option<DeepXWsSupersededCandle>> {
        #[derive(Deserialize)]
        struct Frame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsPerpMarket>,
            subscriptions: Option<Vec<Value>>,
            channel: Option<String>,
            interval: Option<String>,
            data: Option<Box<RawValue>>,
            timestamp: Option<u64>,
        }

        let frame: Frame = serde_json::from_str(text)?;
        if frame.kind == "pong" {
            return Ok(None);
        }
        let market = frame
            .market
            .ok_or_else(|| anyhow::anyhow!("missing DeepX candle market"))?;
        ensure!(
            market.kind == "perp" && market.id == self.market_id,
            "foreign DeepX candle market"
        );
        match frame.kind.as_str() {
            "subscribed" => {
                ensure!(!self.acknowledged, "duplicate DeepX candle acknowledgement");
                let subscriptions = frame
                    .subscriptions
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX candle subscriptions"))?;
                ensure!(
                    subscriptions.len() == 1
                        && (subscriptions[0] == serde_json::json!({"time_frame": "1m"})
                            || subscriptions[0]
                                == serde_json::json!({"channel": "candles", "time_frame": "1m"})),
                    "conflicting DeepX candle acknowledgement"
                );
                self.acknowledged = true;
                Ok(None)
            }
            "data" => {
                ensure!(self.acknowledged, "unacknowledged DeepX candle data");
                ensure!(
                    frame.channel.as_deref() == Some("candles")
                        && frame.interval.as_deref() == Some("1m"),
                    "foreign DeepX candle channel or interval"
                );
                let timestamp = frame
                    .timestamp
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX candle timestamp"))?;
                ensure!(
                    self.last_timestamp_ms
                        .is_none_or(|previous| timestamp >= previous),
                    "DeepX candle update timestamp moved backwards"
                );
                let data = frame
                    .data
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX candle data"))?;
                let page: CandlePage = serde_json::from_str(data.get())?;
                ensure!(page.pair == self.pair, "foreign DeepX candle pair");
                ensure!(
                    page.details.len() == 1,
                    "DeepX candle update must contain one bucket"
                );
                let wire = page
                    .details
                    .into_iter()
                    .next()
                    .expect("checked single candle");
                let candle = DeepXPerpCandle {
                    volume: wire.volume,
                    high: wire.high,
                    low: wire.low,
                    open: wire.open,
                    close: wire.close,
                    time: wire.time,
                };
                ensure!(
                    candle.time > 0
                        && candle.time.is_multiple_of(ONE_MINUTE_MS)
                        && timestamp >= candle.time
                        && candle.open > Decimal::ZERO
                        && candle.high >= candle.open.max(candle.close)
                        && candle.low <= candle.open.min(candle.close)
                        && candle.low > Decimal::ZERO
                        && candle.volume >= Decimal::ZERO,
                    "invalid DeepX candle values or time"
                );
                if let Some(previous) = &self.pending {
                    ensure!(
                        candle.time >= previous.time,
                        "DeepX candle bucket moved backwards"
                    );
                }
                self.last_timestamp_ms = Some(timestamp);
                let Some(previous) = self.pending.replace(candle.clone()) else {
                    return Ok(None);
                };
                if candle.time == previous.time {
                    return Ok(None);
                }
                Ok(Some(DeepXWsSupersededCandle {
                    gap_after: candle.time - previous.time != ONE_MINUTE_MS,
                    candle: previous,
                }))
            }
            _ => anyhow::bail!("unsupported DeepX candle frame"),
        }
    }
}

/// Observes the first superseded one-minute candle on a read-only connection.
///
/// The preceding bucket is never returned merely because wall time crossed its boundary. The
/// connection closes after a newer bucket is observed. This raw observation is not a framework
/// Bar and does not establish continuity across connections or rule out late venue corrections.
///
/// # Errors
///
/// Returns an error for invalid timing, connection, acknowledgement, frame, or deadline failure.
pub async fn observe_first_superseded_perp_candle(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    operation_timeout: Duration,
    observation_timeout: Duration,
    mut stream: DeepXWsCandleStream,
) -> Result<DeepXWsSupersededCandle> {
    ensure!(
        !operation_timeout.is_zero() && !observation_timeout.is_zero(),
        "DeepX candle observation timeouts must be positive"
    );
    let mut connection =
        super::transport::DeepXWsReadConnection::connect(network, proxy_url, operation_timeout)
            .await?;
    if let Err(e) = connection.send_candle_request(&stream).await {
        let _ = connection.close().await;
        return Err(e);
    }
    let result = tokio::time::timeout(observation_timeout, async {
        loop {
            let message = match connection.receive().await {
                Ok(Some(message)) => message,
                Ok(None) => anyhow::bail!("DeepX candle connection closed before a newer bucket"),
                Err(e)
                    if matches!(
                        e.downcast_ref::<super::DeepXWsError>(),
                        Some(super::DeepXWsError::ReceiveTimeout)
                    ) =>
                {
                    continue;
                }
                Err(e) => return Err(e),
            };
            let Message::Text(bytes) = message else {
                if matches!(message, Message::Ping(_) | Message::Pong(_)) {
                    continue;
                }
                anyhow::bail!("unsupported DeepX candle stream message");
            };
            if let Some(completed) = stream.apply(std::str::from_utf8(&bytes)?)? {
                break Ok::<_, anyhow::Error>(completed);
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        Err(anyhow::anyhow!(
            "DeepX candle observation deadline exceeded"
        ))
    });
    let close = connection.close().await;
    match result {
        Ok(completed) => {
            close?;
            Ok(completed)
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        extract::ws::{Message as ServerMessage, WebSocketUpgrade},
        routing::get,
    };
    use rstest::rstest;

    use super::*;

    const ACK: &str = r#"{"type":"subscribed","market":{"type":"perp","id":3,"name":"ETH-USDC"},"subscriptions":[{"time_frame":"1m"}]}"#;

    fn update(time: u64, volume: &str) -> String {
        format!(
            r#"{{"type":"data","channel":"candles","market":{{"type":"perp","id":3}},"interval":"1m","data":{{"details":[{{"close":2434.02,"high":2434.18,"low":2433.67,"open":2433.8,"time":{time},"volume":{volume}}}],"pair":"ETH-USDC"}},"timestamp":{}}}"#,
            time + 17_000
        )
    }

    #[rstest]
    fn emits_only_after_successor_and_preserves_exact_decimals() {
        let mut stream = DeepXWsCandleStream::new(3, "ETH-USDC").unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&stream.subscription_text().unwrap()).unwrap()["subscriptions"]
                [0],
            serde_json::json!({"channel": "candles", "time_frame": "1m"})
        );
        stream.apply(ACK).unwrap();
        assert!(
            stream
                .apply(&update(1_791_477_600_000, "12.559"))
                .unwrap()
                .is_none()
        );
        assert!(
            stream
                .apply(&update(1_791_477_600_000, "12.867000000000000001"))
                .unwrap()
                .is_none()
        );
        let completed = stream
            .apply(&update(1_791_477_660_000, "1.458"))
            .unwrap()
            .unwrap();
        assert_eq!(completed.candle.volume.to_string(), "12.867000000000000001");
        assert!(!completed.gap_after);
        let completed = stream
            .apply(&update(1_791_477_780_000, "1.459"))
            .unwrap()
            .unwrap();
        assert!(completed.gap_after);
    }

    #[rstest]
    fn accepts_exact_scientific_notation_without_floating_point() {
        let mut stream = DeepXWsCandleStream::new(3, "ETH-USDC").unwrap();
        stream.apply(ACK).unwrap();
        stream
            .apply(&update(1_791_477_600_000, "1.2867e1"))
            .unwrap();
        let completed = stream
            .apply(&update(1_791_477_660_000, "1"))
            .unwrap()
            .unwrap();
        assert_eq!(completed.candle.volume, "12.867".parse().unwrap());
    }

    #[rstest]
    fn rejects_unconfirmed_foreign_and_backwards_data() {
        let mut stream = DeepXWsCandleStream::new(3, "ETH-USDC").unwrap();
        assert!(stream.apply(&update(1_791_477_600_000, "1")).is_err());
        assert!(stream.apply(ACK).is_err());

        let mut stream = DeepXWsCandleStream::new(3, "ETH-USDC").unwrap();
        assert!(stream.apply(&ACK.replace("\"id\":3", "\"id\":4")).is_err());

        let mut stream = DeepXWsCandleStream::new(3, "ETH-USDC").unwrap();
        stream.apply(ACK).unwrap();
        assert!(
            stream
                .apply(
                    &update(1_791_477_600_000, "1")
                        .replace("\"interval\":\"1m\"", "\"interval\":\"3m\"")
                )
                .is_err()
        );

        let mut stream = DeepXWsCandleStream::new(3, "ETH-USDC").unwrap();
        stream.apply(ACK).unwrap();
        stream.apply(&update(1_791_477_660_000, "1")).unwrap();
        assert!(stream.apply(&update(1_791_477_600_000, "1")).is_err());
    }

    #[rstest]
    fn rejects_out_of_order_updates_within_one_bucket() {
        let mut stream = DeepXWsCandleStream::new(3, "ETH-USDC").unwrap();
        stream.apply(ACK).unwrap();
        let first = update(1_791_477_600_000, "1");
        let later = first.replace("1791477617000", "1791477618000");
        stream.apply(&later).unwrap();
        assert!(stream.apply(&first).is_err());
        assert!(stream.apply(&update(1_791_477_660_000, "2")).is_err());
    }

    #[rstest]
    fn new_connection_does_not_reuse_an_unfinished_bucket() {
        let mut old = DeepXWsCandleStream::new(3, "ETH-USDC").unwrap();
        old.apply(ACK).unwrap();
        old.apply(&update(1_791_477_600_000, "12.867")).unwrap();

        let mut fresh = DeepXWsCandleStream::new(3, "ETH-USDC").unwrap();
        fresh.apply(ACK).unwrap();
        assert!(
            fresh
                .apply(&update(1_791_477_720_000, "1.458"))
                .unwrap()
                .is_none()
        );
    }

    #[rstest]
    #[case("\"channel\":\"candles\"", "\"channel\":\"trades\"")]
    #[case("\"pair\":\"ETH-USDC\"", "\"pair\":\"BTC-USDC\"")]
    #[case("\"volume\":12.867", "\"volume\":12.867999999999999999999999999999")]
    fn rejects_foreign_or_unrepresentable_updates(
        #[case] original: &str,
        #[case] replacement: &str,
    ) {
        let mut stream = DeepXWsCandleStream::new(3, "ETH-USDC").unwrap();
        stream.apply(ACK).unwrap();
        let frame = update(1_791_477_600_000, "12.867").replace(original, replacement);
        assert!(stream.apply(&frame).is_err());
        assert!(stream.apply(&update(1_791_477_660_000, "1")).is_err());
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[tokio::test]
    async fn observer_requires_ack_and_newer_bucket(#[case] scenario: u8) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    let Some(Ok(ServerMessage::Text(request))) = socket.recv().await else {
                        panic!("expected perpetual candle subscription");
                    };
                    let request: Value = serde_json::from_str(&request).unwrap();
                    assert_eq!(request["market"]["id"], 3);
                    assert_eq!(request["subscriptions"][0]["channel"], "candles");
                    assert_eq!(request["subscriptions"][0]["time_frame"], "1m");
                    if scenario == 1 {
                        let _ = socket
                            .send(ServerMessage::Text(update(1_791_477_600_000, "1").into()))
                            .await;
                    } else {
                        let ack = if scenario == 2 {
                            ACK.replace("\"time_frame\":\"1m\"", "\"time_frame\":\"3m\"")
                        } else {
                            ACK.to_string()
                        };
                        let _ = socket.send(ServerMessage::Text(ack.into())).await;
                        let _ = socket
                            .send(ServerMessage::Text(
                                update(1_791_477_600_000, "12.559").into(),
                            ))
                            .await;
                        if scenario != 3 {
                            let _ = socket
                                .send(ServerMessage::Text(
                                    update(1_791_477_720_000, "1.458").into(),
                                ))
                                .await;
                        }
                    }
                    if scenario == 3 {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let stream = DeepXWsCandleStream::new(3, "ETH-USDC").unwrap();
        let result = observe_first_superseded_perp_candle(
            &network,
            None,
            Duration::from_secs(1),
            Duration::from_millis(100),
            stream,
        )
        .await;
        if scenario == 0 {
            let completed = result.unwrap();
            assert_eq!(completed.candle.volume.to_string(), "12.559");
            assert!(completed.gap_after);
        } else {
            assert!(result.is_err());
        }
        server.abort();
    }
}
