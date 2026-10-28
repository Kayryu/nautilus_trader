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

//! Read-only initial Spot open-order snapshots for one market and address scope.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Result, ensure};
pub use deepx_rust_sdk::websocket::account_spot_orders::{
    DeepXWsSpotOpenOrdersFrame, DeepXWsSpotOpenOrdersPage, DeepXWsSpotOpenOrdersRequest,
};
use nautilus_network::transport::Message;
#[cfg(test)]
use serde_json::json;

use super::transport::DeepXWsReadConnection;
use crate::config::DeepXNetworkConfig;
#[cfg(test)]
use crate::http::DeepXSpotMarket;

/// Reads the first complete page from one acknowledged, read-only Spot subscription.
///
/// The connection closes after the first page. Raw items do not establish finalized order state.
///
/// # Errors
///
/// Returns an error for connection, acknowledgement, identity, page, or deadline failure.
pub async fn observe_initial_spot_open_orders_page(
    network: &DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsSpotOpenOrdersRequest,
) -> Result<(DeepXWsSpotOpenOrdersPage, u64)> {
    ensure!(
        !request.is_wallet(),
        "DeepX Spot single-page observer requires a subaccount request"
    );
    let mut pages =
        observe_initial_spot_open_order_pages(network, proxy_url, timeout, request).await?;
    pages
        .pop()
        .ok_or_else(|| anyhow::anyhow!("DeepX Spot order snapshot is empty"))
}

/// Reads one complete initial page for every expected wallet subaccount.
///
/// The returned pages are ordered by canonical subaccount address. The caller supplies an
/// independently obtained directory; this mutable observation does not prove finalized ownership.
///
/// # Errors
///
/// Returns an error for a missing, duplicated, foreign, or incomplete wallet page.
pub async fn observe_initial_spot_wallet_open_orders_pages(
    network: &DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsSpotOpenOrdersRequest,
) -> Result<Vec<(DeepXWsSpotOpenOrdersPage, u64)>> {
    ensure!(
        request.is_wallet(),
        "DeepX Spot wallet observer requires a wallet request"
    );
    observe_initial_spot_open_order_pages(network, proxy_url, timeout, request).await
}

async fn observe_initial_spot_open_order_pages(
    network: &DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsSpotOpenOrdersRequest,
) -> Result<Vec<(DeepXWsSpotOpenOrdersPage, u64)>> {
    ensure!(
        !timeout.is_zero(),
        "DeepX Spot order subscription timeout must be positive"
    );
    let mut connection = DeepXWsReadConnection::connect(network, proxy_url, timeout).await?;
    connection.send_spot_open_orders_request(request).await?;
    let result = tokio::time::timeout(timeout, async {
        let mut acknowledged = false;
        let mut pages = BTreeMap::new();
        let expected_count = request.expected_page_count();
        loop {
            let Some(message) = connection.receive().await? else {
                anyhow::bail!("DeepX Spot order stream closed before its first page");
            };
            let Message::Text(bytes) = message else {
                if matches!(message, Message::Ping(_) | Message::Pong(_)) {
                    continue;
                }
                anyhow::bail!("unsupported DeepX Spot order stream message");
            };
            match DeepXWsSpotOpenOrdersFrame::parse(std::str::from_utf8(&bytes)?, request)? {
                DeepXWsSpotOpenOrdersFrame::Subscribed => {
                    ensure!(!acknowledged, "duplicate DeepX Spot order acknowledgement");
                    acknowledged = true;
                }
                DeepXWsSpotOpenOrdersFrame::OrderPage { page, timestamp_ms } => {
                    ensure!(acknowledged, "DeepX Spot order page preceded its ack");
                    let address = page.subaccount().to_ascii_lowercase();
                    ensure!(
                        pages.insert(address, (page, timestamp_ms)).is_none(),
                        "duplicate DeepX Spot order page"
                    );
                    if pages.len() == expected_count {
                        break Ok::<_, anyhow::Error>(pages.into_values().collect());
                    }
                }
                DeepXWsSpotOpenOrdersFrame::Pong => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("DeepX Spot order page deadline exceeded")));
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
    const PAIR: &str = "0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37";
    const ACK: &str = r#"{"type":"subscribed","market":{"type":"spot","name":"ETH/USDC","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"},"subscriptions":[{"channel":"user_spot_open_orders","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"}],"message":"Successfully subscribed to 1 channels"}"#;
    const SNAPSHOT: &str = r#"{"type":"data","channel":"user_spot_open_orders","market":{"type":"spot","name":"ETH/USDC","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","orders":{"hasNext":false,"hasPrev":false,"items":[],"pageNo":1,"pageSize":100,"totalCount":0,"totalPage":0}},"timestamp":1791491471048}"#;
    const MARKETS: &str =
        include_str!("../../deepx-rust-sdk/test_data/http/testnet/spot_markets.json");

    fn market() -> DeepXSpotMarket {
        let response: crate::http::DeepXApiResponse<Vec<DeepXSpotMarket>> =
            serde_json::from_str(MARKETS).unwrap();
        response
            .data
            .into_iter()
            .find(|market| market.name == "ETH/USDC")
            .unwrap()
    }

    fn request() -> DeepXWsSpotOpenOrdersRequest {
        DeepXWsSpotOpenOrdersRequest::new(&market(), ADDRESS).unwrap()
    }

    fn wallet_request() -> DeepXWsSpotOpenOrdersRequest {
        DeepXWsSpotOpenOrdersRequest::for_wallet(
            &market(),
            WALLET,
            &[ADDRESS.to_string(), OTHER.to_string()],
        )
        .unwrap()
    }

    #[rstest]
    fn parses_observed_ack_and_empty_snapshot() {
        let request = request();
        let encoded: serde_json::Value = serde_json::from_str(&request.to_text()).unwrap();
        assert_eq!(encoded["market"]["name"], "ETH/USDC");
        assert_eq!(encoded["subscriptions"][0]["address"], ADDRESS);
        assert_eq!(encoded["options"]["compress"], false);
        assert!(matches!(
            DeepXWsSpotOpenOrdersFrame::parse(ACK, &request).unwrap(),
            DeepXWsSpotOpenOrdersFrame::Subscribed
        ));
        let DeepXWsSpotOpenOrdersFrame::OrderPage { page, timestamp_ms } =
            DeepXWsSpotOpenOrdersFrame::parse(SNAPSHOT, &request).unwrap()
        else {
            panic!("expected initial Spot order page");
        };
        assert!(page.items().is_empty());
        assert_eq!(page.subaccount(), ADDRESS);
        assert_eq!(timestamp_ms, 1_791_491_471_048);
    }

    #[rstest]
    fn preserves_nonempty_raw_item_with_bound_identity() {
        let item = format!(
            "{{\"orderId\":\"123\",\"maker\":\"{ADDRESS}\",\"pair\":\"{PAIR}\",\"price\":1.2300}}"
        );
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":[{item}]"))
            .replace("\"totalCount\":0", "\"totalCount\":1")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        let DeepXWsSpotOpenOrdersFrame::OrderPage { page, .. } =
            DeepXWsSpotOpenOrdersFrame::parse(&frame, &request()).unwrap()
        else {
            panic!("expected initial Spot order page");
        };
        assert_eq!(page.items()[0].get(), item);
    }

    #[rstest]
    #[case("00", ADDRESS, PAIR)]
    #[case("1.0", ADDRESS, PAIR)]
    #[case("1", "0x0000000000000000000000000000000000000000", PAIR)]
    #[case(
        "1",
        ADDRESS,
        "0x0000000000000000000000000000000000000000000000000000000000000000"
    )]
    fn rejects_invalid_order_identity(
        #[case] order_id: &str,
        #[case] maker: &str,
        #[case] pair: &str,
    ) {
        let item =
            format!("{{\"orderId\":\"{order_id}\",\"maker\":\"{maker}\",\"pair\":\"{pair}\"}}");
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":[{item}]"))
            .replace("\"totalCount\":0", "\"totalCount\":1")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        assert!(DeepXWsSpotOpenOrdersFrame::parse(&frame, &request()).is_err());
    }

    #[rstest]
    fn rejects_duplicate_numeric_order_ids() {
        let items = format!(
            "[{{\"orderId\":\"1\",\"maker\":\"{ADDRESS}\",\"pair\":\"{PAIR}\"}},{{\"orderId\":\"01\",\"maker\":\"{ADDRESS}\",\"pair\":\"{PAIR}\"}}]"
        );
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":{items}"))
            .replace("\"totalCount\":0", "\"totalCount\":2")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        assert!(DeepXWsSpotOpenOrdersFrame::parse(&frame, &request()).is_err());
    }

    #[rstest]
    fn accepts_first_generated_order_id_zero() {
        let item = format!("{{\"orderId\":\"0\",\"maker\":\"{ADDRESS}\",\"pair\":\"{PAIR}\"}}");
        let frame = SNAPSHOT
            .replace("\"items\":[]", &format!("\"items\":[{item}]"))
            .replace("\"totalCount\":0", "\"totalCount\":1")
            .replace("\"totalPage\":0", "\"totalPage\":1");
        let DeepXWsSpotOpenOrdersFrame::OrderPage { page, .. } =
            DeepXWsSpotOpenOrdersFrame::parse(&frame, &request()).unwrap()
        else {
            panic!("expected zero-ID Spot order page");
        };
        assert_eq!(page.items()[0].get(), item);
    }

    #[rstest]
    #[case(
        "pair",
        "\"pair\":\"0x0000000000000000000000000000000000000000000000000000000000000000\""
    )]
    #[case(
        "address",
        "\"address\":\"0x0000000000000000000000000000000000000000\""
    )]
    #[case("channel", "\"channel\":\"user_spot_order_history\"")]
    #[case("pagination", "\"hasNext\":true")]
    fn rejects_foreign_or_incomplete_snapshot(#[case] field: &str, #[case] replacement: &str) {
        let original = match field {
            "pair" => format!("\"pair\":\"{PAIR}\""),
            "address" => format!("\"address\":\"{ADDRESS}\""),
            "channel" => "\"channel\":\"user_spot_open_orders\"".to_string(),
            "pagination" => "\"hasNext\":false".to_string(),
            _ => unreachable!(),
        };
        let changed = SNAPSHOT.replacen(&original, replacement, 1);
        assert_ne!(changed, SNAPSHOT);
        assert!(DeepXWsSpotOpenOrdersFrame::parse(&changed, &request()).is_err());
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[tokio::test]
    async fn first_page_requires_one_matching_ack(#[case] scenario: u8) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    let Some(Ok(ServerMessage::Text(request))) = socket.recv().await else {
                        panic!("expected Spot order subscription");
                    };
                    let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                    assert_eq!(
                        request["subscriptions"][0]["channel"],
                        "user_spot_open_orders"
                    );
                    assert_eq!(request["market"]["name"], "ETH/USDC");
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
        let network = DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let result = observe_initial_spot_open_orders_page(
            &network,
            None,
            Duration::from_secs(2),
            &request(),
        )
        .await;
        if scenario == 0 {
            let (page, timestamp_ms) = result.unwrap();
            assert_eq!(page.subaccount(), ADDRESS);
            assert!(page.items().is_empty());
            assert_eq!(timestamp_ms, 1_791_491_471_048);
        } else {
            assert!(result.is_err());
        }
        server.abort();
    }

    #[rstest]
    fn wallet_request_requires_valid_unique_subaccounts() {
        assert!(DeepXWsSpotOpenOrdersRequest::for_wallet(&market(), WALLET, &[]).is_err());
        assert!(
            DeepXWsSpotOpenOrdersRequest::for_wallet(
                &market(),
                WALLET,
                &[ADDRESS.to_string(), ADDRESS.to_string()],
            )
            .is_err()
        );
        assert!(
            DeepXWsSpotOpenOrdersRequest::for_wallet(&market(), WALLET, &["invalid".to_string()],)
                .is_err()
        );
        let encoded: serde_json::Value = serde_json::from_str(&wallet_request().to_text()).unwrap();
        assert_eq!(encoded["subscriptions"][0]["address"], WALLET);
        assert_eq!(encoded["subscriptions"][0]["addressType"], "wallet");
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
                        panic!("expected Spot wallet subscription");
                    };
                    let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                    assert_eq!(request["subscriptions"][0]["address"], WALLET);
                    assert_eq!(request["subscriptions"][0]["addressType"], "wallet");
                    let mut ack: serde_json::Value = serde_json::from_str(ACK).unwrap();
                    if scenario != 4 {
                        ack["subscriptions"]
                            .as_array_mut()
                            .unwrap()
                            .push(json!({"channel": "user_spot_open_orders", "address": OTHER}));
                    }
                    if scenario == 5 {
                        ack["subscriptions"][1]["address"] = json!(ADDRESS);
                    } else if scenario == 6 {
                        ack["subscriptions"][1]["address"] =
                            json!("0x0000000000000000000000000000000000000000");
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
        let network = DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let result = observe_initial_spot_wallet_open_orders_pages(
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
        let network = DeepXNetworkConfig::default();
        assert!(
            observe_initial_spot_open_orders_page(
                &network,
                None,
                Duration::from_secs(1),
                &wallet_request(),
            )
            .await
            .is_err()
        );
        assert!(
            observe_initial_spot_wallet_open_orders_pages(
                &network,
                None,
                Duration::from_secs(1),
                &request(),
            )
            .await
            .is_err()
        );
    }
}
