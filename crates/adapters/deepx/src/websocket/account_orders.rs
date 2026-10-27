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

//! Read-only perpetual open-order subscription envelopes.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Result, ensure};
pub use deepx_rust_sdk::websocket::account_orders::{
    DeepXWsPerpOpenOrdersFrame, DeepXWsPerpOpenOrdersPage, DeepXWsPerpOpenOrdersRequest,
};
use nautilus_network::transport::Message;

/// Reads the first complete order page from one acknowledged, read-only subscription.
///
/// The connection closes after the first page. Order items retain their raw JSON and cannot
/// establish canonical order state or authorize framework reports.
///
/// # Errors
///
/// Returns an error for connection, acknowledgement, identity, page, or deadline failure.
pub async fn observe_initial_perp_open_orders_page(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsPerpOpenOrdersRequest,
) -> Result<(DeepXWsPerpOpenOrdersPage, u64)> {
    ensure!(
        !request.is_wallet(),
        "DeepX perpetual single-page observer requires a subaccount request"
    );
    let mut pages =
        observe_initial_perp_open_order_pages(network, proxy_url, timeout, request).await?;
    pages
        .pop()
        .ok_or_else(|| anyhow::anyhow!("DeepX perpetual order snapshot is empty"))
}

/// Reads one complete initial order page for every expected wallet subaccount.
///
/// The pages are sorted by subaccount address. This mutable observation does not prove finalized
/// ownership or order state.
///
/// # Errors
///
/// Returns an error for a missing, duplicated, foreign, or incomplete page.
pub async fn observe_initial_perp_wallet_open_orders_pages(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsPerpOpenOrdersRequest,
) -> Result<Vec<(DeepXWsPerpOpenOrdersPage, u64)>> {
    ensure!(
        request.is_wallet(),
        "DeepX perpetual wallet observer requires a wallet request"
    );
    observe_initial_perp_open_order_pages(network, proxy_url, timeout, request).await
}

async fn observe_initial_perp_open_order_pages(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsPerpOpenOrdersRequest,
) -> Result<Vec<(DeepXWsPerpOpenOrdersPage, u64)>> {
    ensure!(
        !timeout.is_zero(),
        "DeepX order subscription timeout must be positive"
    );
    let mut connection =
        super::transport::DeepXWsReadConnection::connect(network, proxy_url, timeout).await?;
    connection.send_perp_open_orders_request(request).await?;
    let result = tokio::time::timeout(timeout, async {
        let mut acknowledged = false;
        let mut pages = BTreeMap::new();
        let expected_count = request.expected_page_count();
        loop {
            let Some(message) = connection.receive().await? else {
                anyhow::bail!("DeepX perpetual order stream closed before its first page");
            };
            let Message::Text(bytes) = message else {
                if matches!(message, Message::Ping(_) | Message::Pong(_)) {
                    continue;
                }
                anyhow::bail!("unsupported DeepX perpetual order stream message");
            };
            match DeepXWsPerpOpenOrdersFrame::parse(std::str::from_utf8(&bytes)?, request)? {
                DeepXWsPerpOpenOrdersFrame::Subscribed => {
                    ensure!(
                        !acknowledged,
                        "duplicate DeepX perpetual order acknowledgement"
                    );
                    acknowledged = true;
                }
                DeepXWsPerpOpenOrdersFrame::OrderPage { page, timestamp_ms } => {
                    ensure!(acknowledged, "DeepX perpetual order page preceded its ack");
                    let address = page.subaccount().to_ascii_lowercase();
                    ensure!(
                        pages.insert(address, (page, timestamp_ms)).is_none(),
                        "duplicate DeepX perpetual order page"
                    );
                    if pages.len() == expected_count {
                        break Ok::<_, anyhow::Error>(pages.into_values().collect());
                    }
                }
                DeepXWsPerpOpenOrdersFrame::Pong => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        Err(anyhow::anyhow!(
            "DeepX perpetual order page deadline exceeded"
        ))
    });
    let close = connection.close().await;
    result.and_then(|pages| close.map(|()| pages))
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
    const OTHER: &str = "0x977654069311fa41b88f7901dc65fae9a76fd60b";
    const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
    const ACK: &str = r#"{"type":"subscribed","market":{"type":"perp","id":3,"name":"ETH-USDC"},"subscriptions":[{"channel":"user_perp_open_orders","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"}],"message":"Successfully subscribed to 1 channels"}"#;
    const SNAPSHOT: &str = r#"{"type":"data","channel":"user_perp_open_orders","market":{"type":"perp","id":3,"name":"ETH-USDC"},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","orders":{"hasNext":false,"hasPrev":false,"items":[],"pageNo":1,"pageSize":100,"totalCount":0,"totalPage":0}},"timestamp":1791475653766}"#;

    fn wallet_request() -> DeepXWsPerpOpenOrdersRequest {
        DeepXWsPerpOpenOrdersRequest::for_wallet(
            3,
            WALLET,
            &[ADDRESS.to_string(), OTHER.to_string()],
        )
        .unwrap()
    }

    #[rstest]
    fn parses_observed_ack_and_empty_snapshot() {
        let request = DeepXWsPerpOpenOrdersRequest::new(3, ADDRESS).unwrap();
        let encoded: serde_json::Value = serde_json::from_str(&request.to_text().unwrap()).unwrap();
        assert_eq!(encoded["market"]["id"], 3);
        assert_eq!(encoded["subscriptions"][0]["address"], ADDRESS);
        assert!(matches!(
            DeepXWsPerpOpenOrdersFrame::parse(ACK, &request).unwrap(),
            DeepXWsPerpOpenOrdersFrame::Subscribed
        ));
        let DeepXWsPerpOpenOrdersFrame::OrderPage { page, timestamp_ms } =
            DeepXWsPerpOpenOrdersFrame::parse(SNAPSHOT, &request).unwrap()
        else {
            panic!("expected initial snapshot");
        };
        assert!(page.items().is_empty());
        assert_eq!(page.subaccount(), ADDRESS);
        assert_eq!(timestamp_ms, 1_791_475_653_766);
    }

    #[rstest]
    fn preserves_nonempty_order_items_without_assigning_report_semantics() {
        let request = DeepXWsPerpOpenOrdersRequest::new(3, ADDRESS).unwrap();
        let item = format!(
            "{{\"orderId\":\"1791486151624\",\"owner\":\"{ADDRESS}\",\"marketId\":3,\"price\":1.2300}}"
        );
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":[{item}]"))
            .replace("\"totalCount\":0", "\"totalCount\":1")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        let DeepXWsPerpOpenOrdersFrame::OrderPage { page, .. } =
            DeepXWsPerpOpenOrdersFrame::parse(&frame, &request).unwrap()
        else {
            panic!("expected initial snapshot");
        };
        assert_eq!(page.items()[0].get(), item);
    }

    #[rstest]
    #[case("00", ADDRESS, 3)]
    #[case("1.0", ADDRESS, 3)]
    #[case("1", "0x0000000000000000000000000000000000000000", 3)]
    #[case("1", ADDRESS, 4)]
    fn rejects_invalid_order_item_identity(
        #[case] order_id: &str,
        #[case] owner: &str,
        #[case] market_id: u64,
    ) {
        let request = DeepXWsPerpOpenOrdersRequest::new(3, ADDRESS).unwrap();
        let item = format!(
            "{{\"orderId\":\"{order_id}\",\"owner\":\"{owner}\",\"marketId\":{market_id}}}"
        );
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":[{item}]"))
            .replace("\"totalCount\":0", "\"totalCount\":1")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        assert!(DeepXWsPerpOpenOrdersFrame::parse(&frame, &request).is_err());
    }

    #[rstest]
    fn rejects_duplicate_numeric_order_ids() {
        let request = DeepXWsPerpOpenOrdersRequest::new(3, ADDRESS).unwrap();
        let items = format!(
            "[{{\"orderId\":\"1\",\"owner\":\"{ADDRESS}\",\"marketId\":3}},{{\"orderId\":\"01\",\"owner\":\"{ADDRESS}\",\"marketId\":3}}]"
        );
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":{items}"))
            .replace("\"totalCount\":0", "\"totalCount\":2")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        assert!(DeepXWsPerpOpenOrdersFrame::parse(&frame, &request).is_err());
    }

    #[rstest]
    fn accepts_first_generated_order_id_zero() {
        let request = DeepXWsPerpOpenOrdersRequest::new(3, ADDRESS).unwrap();
        let item = format!("{{\"orderId\":\"0\",\"owner\":\"{ADDRESS}\",\"marketId\":3}}");
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":[{item}]"))
            .replace("\"totalCount\":0", "\"totalCount\":1")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        let DeepXWsPerpOpenOrdersFrame::OrderPage { page, .. } =
            DeepXWsPerpOpenOrdersFrame::parse(&frame, &request).unwrap()
        else {
            panic!("expected zero-ID order page");
        };
        assert_eq!(page.items()[0].get(), item);
    }

    #[rstest]
    #[case("market", "\"id\":4")]
    #[case(
        "address",
        "\"address\":\"0x0000000000000000000000000000000000000000\""
    )]
    #[case("channel", "\"channel\":\"user_perp_trade_history\"")]
    #[case("pagination", "\"hasNext\":true")]
    fn rejects_foreign_or_incomplete_snapshot(#[case] field: &str, #[case] replacement: &str) {
        let request = DeepXWsPerpOpenOrdersRequest::new(3, ADDRESS).unwrap();
        let original = match field {
            "market" => "\"id\":3".to_string(),
            "address" => format!("\"address\":\"{ADDRESS}\""),
            "channel" => "\"channel\":\"user_perp_open_orders\"".to_string(),
            "pagination" => "\"hasNext\":false".to_string(),
            _ => unreachable!(),
        };
        let changed = SNAPSHOT.replacen(&original, replacement, 1);
        assert_ne!(changed, SNAPSHOT);
        assert!(DeepXWsPerpOpenOrdersFrame::parse(&changed, &request).is_err());
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[tokio::test]
    async fn initial_page_requires_one_matching_ack(#[case] scenario: u8) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    let Some(Ok(ServerMessage::Text(request))) = socket.recv().await else {
                        panic!("expected perpetual order subscription");
                    };
                    let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                    assert_eq!(
                        request["subscriptions"][0]["channel"],
                        "user_perp_open_orders"
                    );
                    assert_eq!(request["market"]["id"], 3);
                    let ack = if scenario == 2 {
                        ACK.replace(ADDRESS, "0x0000000000000000000000000000000000000000")
                    } else {
                        ACK.to_string()
                    };
                    if scenario == 1 {
                        let _ = socket.send(ServerMessage::Text(SNAPSHOT.into())).await;
                    }
                    let _ = socket.send(ServerMessage::Text(ack.clone().into())).await;
                    if scenario == 3 {
                        let _ = socket.send(ServerMessage::Text(ack.into())).await;
                    }
                    let _ = socket.send(ServerMessage::Text(SNAPSHOT.into())).await;
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let request = DeepXWsPerpOpenOrdersRequest::new(3, ADDRESS).unwrap();
        let result =
            observe_initial_perp_open_orders_page(&network, None, Duration::from_secs(2), &request)
                .await;
        if scenario == 0 {
            let (page, timestamp_ms) = result.unwrap();
            assert_eq!(page.subaccount(), ADDRESS);
            assert!(page.items().is_empty());
            assert_eq!(timestamp_ms, 1_791_475_653_766);
        } else {
            assert!(result.is_err());
        }
        server.abort();
    }

    #[rstest]
    fn wallet_request_requires_valid_unique_subaccounts() {
        assert!(DeepXWsPerpOpenOrdersRequest::for_wallet(3, WALLET, &[]).is_err());
        assert!(
            DeepXWsPerpOpenOrdersRequest::for_wallet(
                3,
                WALLET,
                &[ADDRESS.to_string(), ADDRESS.to_string()],
            )
            .is_err()
        );
        assert!(DeepXWsPerpOpenOrdersRequest::for_wallet(3, WALLET, &["bad".to_string()]).is_err());
        let encoded: serde_json::Value =
            serde_json::from_str(&wallet_request().to_text().unwrap()).unwrap();
        assert_eq!(encoded["subscriptions"][0]["address"], WALLET);
        assert_eq!(encoded["subscriptions"][0]["addressType"], "wallet");
    }

    #[rstest]
    fn wallet_order_identity_is_bound_to_page_subaccount() {
        let item = format!("{{\"orderId\":\"123\",\"owner\":\"{OTHER}\",\"marketId\":3}}");
        let frame = SNAPSHOT
            .replace(ADDRESS, OTHER)
            .replace("\"items\":[]", &format!("\"items\":[{item}]"))
            .replace("\"totalCount\":0", "\"totalCount\":1")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        let DeepXWsPerpOpenOrdersFrame::OrderPage { page, .. } =
            DeepXWsPerpOpenOrdersFrame::parse(&frame, &wallet_request()).unwrap()
        else {
            panic!("expected wallet order page");
        };
        assert_eq!(page.subaccount(), OTHER);
        assert_eq!(page.items()[0].get(), item);
        assert!(
            DeepXWsPerpOpenOrdersFrame::parse(
                &frame,
                &DeepXWsPerpOpenOrdersRequest::new(3, ADDRESS).unwrap()
            )
            .is_err()
        );
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[tokio::test]
    async fn wallet_snapshot_requires_exact_expansion_and_all_pages(#[case] scenario: u8) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    let Some(Ok(ServerMessage::Text(request))) = socket.recv().await else {
                        panic!("expected perpetual wallet order subscription");
                    };
                    let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                    assert_eq!(request["subscriptions"][0]["address"], WALLET);
                    assert_eq!(request["subscriptions"][0]["addressType"], "wallet");
                    let mut ack: serde_json::Value = serde_json::from_str(ACK).unwrap();
                    if scenario != 4 {
                        ack["subscriptions"]
                            .as_array_mut()
                            .unwrap()
                            .push(serde_json::json!({
                                "channel": "user_perp_open_orders",
                                "address": OTHER,
                            }));
                    }
                    if scenario == 5 {
                        ack["subscriptions"][1]["address"] = serde_json::json!(ADDRESS);
                    } else if scenario == 6 {
                        ack["subscriptions"][1]["address"] =
                            serde_json::json!("0x0000000000000000000000000000000000000000");
                    }
                    let _ = socket
                        .send(ServerMessage::Text(ack.to_string().into()))
                        .await;
                    let _ = socket.send(ServerMessage::Text(SNAPSHOT.into())).await;
                    let other_page = SNAPSHOT.replace(ADDRESS, OTHER);
                    if scenario == 2 {
                        let _ = socket.send(ServerMessage::Text(SNAPSHOT.into())).await;
                    } else if scenario == 3 {
                        let foreign =
                            other_page.replace(OTHER, "0x0000000000000000000000000000000000000000");
                        let _ = socket.send(ServerMessage::Text(foreign.into())).await;
                    } else if scenario != 1 {
                        let _ = socket.send(ServerMessage::Text(other_page.into())).await;
                    }
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let result = observe_initial_perp_wallet_open_orders_pages(
            &network,
            None,
            Duration::from_secs(2),
            &wallet_request(),
        )
        .await;
        if scenario == 0 {
            let pages = result.unwrap();
            assert_eq!(pages.len(), 2);
            assert_eq!(pages[0].0.subaccount(), ADDRESS);
            assert_eq!(pages[1].0.subaccount(), OTHER);
        } else {
            assert!(result.is_err());
        }
        server.abort();
    }

    #[rstest]
    #[tokio::test]
    async fn observers_reject_wrong_request_scope() {
        let network = crate::config::DeepXNetworkConfig::default();
        assert!(
            observe_initial_perp_open_orders_page(
                &network,
                None,
                Duration::from_secs(1),
                &wallet_request(),
            )
            .await
            .is_err()
        );
        assert!(
            observe_initial_perp_wallet_open_orders_pages(
                &network,
                None,
                Duration::from_secs(1),
                &DeepXWsPerpOpenOrdersRequest::new(3, ADDRESS).unwrap(),
            )
            .await
            .is_err()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn deadline_closes_perpetual_order_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (close_tx, mut close_rx) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| {
                let close_tx = close_tx.clone();
                async move {
                    upgrade.on_upgrade(move |mut socket| async move {
                        assert!(matches!(
                            socket.recv().await,
                            Some(Ok(ServerMessage::Text(_)))
                        ));
                        socket.send(ServerMessage::Text(ACK.into())).await.unwrap();
                        let mut heartbeat = tokio::time::interval(Duration::from_millis(20));
                        loop {
                            tokio::select! {
                                message = socket.recv() => {
                                    let closed = matches!(message, Some(Ok(ServerMessage::Close(_))));
                                    let _ = close_tx.send(closed);
                                    break;
                                }
                                _ = heartbeat.tick() => {
                                    if socket.send(ServerMessage::Pong(Vec::new().into())).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    })
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let request = DeepXWsPerpOpenOrdersRequest::new(3, ADDRESS).unwrap();
        let result = observe_initial_perp_open_orders_page(
            &network,
            None,
            Duration::from_millis(200),
            &request,
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("deadline exceeded")
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), close_rx.recv())
                .await
                .unwrap()
                .unwrap()
        );
        server.abort();
    }
}
