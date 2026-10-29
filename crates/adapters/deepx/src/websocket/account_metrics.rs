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

//! Read-only initial account summary observations without financial interpretation.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Result, ensure};
use deepx_rust_sdk::websocket::account_metrics::DeepXWsAccountMetricsFrame as Frame;
pub use deepx_rust_sdk::websocket::account_metrics::{
    DeepXWsAccountMetric, DeepXWsAccountMetricChannel, DeepXWsAccountMetricsRequest,
    DeepXWsExactPortfolioMargin,
};
use nautilus_network::transport::Message;
#[cfg(test)]
use serde_json::Value;

use super::transport::DeepXWsReadConnection;
use crate::config::DeepXNetworkConfig;

/// Reads one initial raw value from each of four acknowledged account summary channels.
///
/// Returned values are ordered by channel, not asserted to be an atomic snapshot. They do not
/// establish exact financial balances or finalized account state.
///
/// # Errors
///
/// Returns an error for connection, acknowledgement, identity, data, or deadline failure.
pub async fn observe_initial_account_metrics(
    network: &DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsAccountMetricsRequest,
) -> Result<Vec<DeepXWsAccountMetric>> {
    ensure!(
        !timeout.is_zero(),
        "DeepX account summary timeout must be positive"
    );
    let mut connection = DeepXWsReadConnection::connect(network, proxy_url, timeout).await?;
    if let Err(e) = connection.send_account_metrics_request(request).await {
        let _ = connection.close().await;
        return Err(e);
    }
    let result = tokio::time::timeout(timeout, async {
        let mut acknowledged = false;
        let mut pre_ack_frames = 0;
        let expected_count = DeepXWsAccountMetricChannel::ALL.len();
        let mut metrics: BTreeMap<DeepXWsAccountMetricChannel, DeepXWsAccountMetric> =
            BTreeMap::new();
        loop {
            let message = match connection.receive().await {
                Ok(Some(message)) => message,
                Ok(None) => anyhow::bail!("DeepX account summary stream closed before completion"),
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
                anyhow::bail!("unsupported DeepX account summary stream message");
            };
            match Frame::parse(std::str::from_utf8(&bytes)?, request)? {
                Frame::Subscribed => {
                    ensure!(
                        !acknowledged,
                        "duplicate DeepX account summary acknowledgement"
                    );
                    acknowledged = true;
                    if metrics.len() == expected_count {
                        break Ok::<_, anyhow::Error>(metrics.into_values().collect());
                    }
                }
                Frame::Metric(metric) => {
                    if !acknowledged {
                        pre_ack_frames += 1;
                        ensure!(
                            pre_ack_frames <= expected_count,
                            "DeepX account summary pre-ack buffer overflow"
                        );
                    }
                    let key = metric.channel();
                    if let Some(previous) = metrics.get(&key) {
                        ensure!(
                            previous.raw_data().get() == metric.raw_data().get(),
                            "conflicting duplicate DeepX account summary data"
                        );
                        continue;
                    }
                    metrics.insert(key, *metric);
                    if acknowledged && metrics.len() == expected_count {
                        break Ok::<_, anyhow::Error>(metrics.into_values().collect());
                    }
                }
                Frame::Pong => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("DeepX account summary deadline exceeded")));
    let close = connection.close().await;
    match result {
        Ok(metrics) => {
            close?;
            Ok(metrics)
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

    const ADDRESS: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
    const ACK: &str = r#"{"type":"subscribed","market":{"type":"all"},"subscriptions":[{"channel":"user_total_deposits","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"},{"channel":"user_net_value","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"},{"channel":"user_equity","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"},{"channel":"user_portfolio_margin_ratio","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"}]}"#;
    const TOTAL: &str = r#"{"type":"data","channel":"user_total_deposits","market":{"type":"all"},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","totalBorrowInterestUsd":0.0,"totalBorrowsUsd":0.0,"totalDepositsUsd":970.9600},"timestamp":1791494312743}"#;
    const NET: &str = r#"{"type":"data","channel":"user_net_value","market":{"type":"all"},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","netValueUsd":970.96,"totalBorrowInterestUsd":0.0,"totalBorrowsUsd":0.0,"totalDepositsUsd":970.96,"unrealizedPnlUsd":0.0},"timestamp":1791494312743}"#;
    const EQUITY: &str = r#"{"type":"data","channel":"user_equity","market":{"type":"all"},"data":{"equityUsd":970.96,"subaccount":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","totalBorrowsUsd":0.0,"totalDepositsUsd":970.96,"unrealizedPnlUsd":0.0},"timestamp":1791494312743}"#;
    const MARGIN: &str = r#"{"type":"data","channel":"user_portfolio_margin_ratio","market":{"type":"all"},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","availableMargin":970.96,"exactMargin":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","availableMargin":"970.963729","initCollateral":"970.963729","initMarginReq":"0","maintMarginReq":"0","marginRatio":null,"totalCollateral":"970.963729","totalMaintenanceMarginRequired":"0","totalMarginRequired":"0","uniIMR":null},"initCollateral":970.96,"initMarginReq":0.0,"maintMarginReq":0.0,"marginRatio":null,"totalCollateral":970.96,"totalMaintenanceMarginRequired":0.0,"totalMarginRequired":0.0,"uniIMR":null},"timestamp":1791494845977}"#;

    fn request() -> DeepXWsAccountMetricsRequest {
        DeepXWsAccountMetricsRequest::new(ADDRESS).unwrap()
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[tokio::test]
    async fn observer_requires_exact_ack_and_all_four_summaries(#[case] scenario: u8) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    let Some(Ok(ServerMessage::Text(request))) = socket.recv().await else {
                        panic!("expected account summary subscription");
                    };
                    let request: Value = serde_json::from_str(&request).unwrap();
                    assert_eq!(request["subscriptions"].as_array().unwrap().len(), 4);
                    if scenario == 1 {
                        let _ = socket.send(ServerMessage::Text(TOTAL.into())).await;
                    } else if scenario == 7 {
                        for _ in 0..5 {
                            let _ = socket.send(ServerMessage::Text(TOTAL.into())).await;
                        }
                    } else if scenario == 8 || scenario == 9 {
                        for frame in [TOTAL, NET, EQUITY, MARGIN] {
                            let _ = socket.send(ServerMessage::Text(frame.into())).await;
                        }
                        if scenario == 9 {
                            return;
                        }
                    }
                    let ack = if scenario == 2 {
                        ACK.replace(ADDRESS, "0x0000000000000000000000000000000000000000")
                    } else if scenario == 6 {
                        let mut ack: Value = serde_json::from_str(ACK).unwrap();
                        ack["subscriptions"].as_array_mut().unwrap().pop();
                        ack.to_string()
                    } else {
                        ACK.to_string()
                    };
                    let _ = socket.send(ServerMessage::Text(ack.into())).await;
                    let _ = socket.send(ServerMessage::Text(TOTAL.into())).await;
                    if scenario == 3 {
                        let _ = socket.send(ServerMessage::Text(TOTAL.into())).await;
                    } else if scenario == 4 {
                        let conflicting = TOTAL.replace("970.9600", "970.9700");
                        let _ = socket.send(ServerMessage::Text(conflicting.into())).await;
                    }
                    let _ = socket.send(ServerMessage::Text(NET.into())).await;
                    let _ = socket.send(ServerMessage::Text(EQUITY.into())).await;
                    if scenario != 5 {
                        let _ = socket.send(ServerMessage::Text(MARGIN.into())).await;
                    }
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let result =
            observe_initial_account_metrics(&network, None, Duration::from_secs(2), &request())
                .await;
        if matches!(scenario, 0 | 1 | 3 | 8) {
            let metrics = result.unwrap();
            assert_eq!(metrics.len(), 4);
            assert_eq!(
                metrics[0].channel(),
                DeepXWsAccountMetricChannel::TotalDeposits
            );
            assert_eq!(metrics[1].channel(), DeepXWsAccountMetricChannel::NetValue);
            assert_eq!(metrics[2].channel(), DeepXWsAccountMetricChannel::Equity);
            assert_eq!(
                metrics[3].channel(),
                DeepXWsAccountMetricChannel::PortfolioMarginRatio
            );
        } else {
            assert!(result.is_err());
        }
        server.abort();
    }
}
