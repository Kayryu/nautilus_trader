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
pub use deepx_rust_sdk::websocket::candles::{DeepXWsCandleStream, DeepXWsSupersededCandle};
use nautilus_network::transport::Message;
#[cfg(test)]
use serde_json::Value;

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
