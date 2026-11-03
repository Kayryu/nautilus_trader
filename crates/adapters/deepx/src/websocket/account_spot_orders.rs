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

use std::time::Duration;

use anyhow::{Result, ensure};
use deepx_rust_sdk::websocket::account_spot_orders::DeepXWsSpotOpenOrdersSnapshot;
pub use deepx_rust_sdk::websocket::account_spot_orders::{
    DeepXWsSpotOpenOrdersFrame, DeepXWsSpotOpenOrdersPage, DeepXWsSpotOpenOrdersRequest,
};
use nautilus_network::transport::Message;

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
        let mut snapshot = DeepXWsSpotOpenOrdersSnapshot::new(request);
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
            let frame = DeepXWsSpotOpenOrdersFrame::parse(std::str::from_utf8(&bytes)?, request)?;
            if let Some(pages) = snapshot.accept(frame)? {
                break Ok::<_, anyhow::Error>(pages);
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
    const ACK: &str = r#"{"type":"subscribed","market":{"type":"spot","name":"ETH/USDC","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"},"subscriptions":[{"channel":"user_spot_open_orders","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"}],"message":"Successfully subscribed to 1 channels"}"#;
    const SNAPSHOT: &str = r#"{"type":"data","channel":"user_spot_open_orders","market":{"type":"spot","name":"ETH/USDC","pair":"0x9068d4ac891a14784c17877eb74bd8489b3367c71d72766dbfa4dfbfb662fa37"},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","orders":{"hasNext":false,"hasPrev":false,"items":[],"pageNo":1,"pageSize":100,"totalCount":0,"totalPage":0}},"timestamp":1791491471048}"#;
    const MARKETS: &str =
        include_str!("../../../../../deepx-rust-sdk/test_data/http/testnet/spot_markets.json");

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
    #[case(0)]
    #[case(1)]
    #[tokio::test]
    async fn first_page_requires_ack_before_snapshot(#[case] scenario: u8) {
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
                    if scenario == 1 {
                        let _ = socket.send(ServerMessage::Text(SNAPSHOT.into())).await;
                    }
                    let _ = socket.send(ServerMessage::Text(ACK.into())).await;
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
    #[case(0)]
    #[case(1)]
    #[tokio::test]
    async fn wallet_snapshot_requires_all_pages(#[case] scenario: u8) {
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
                    ack["subscriptions"].as_array_mut().unwrap().push(
                        serde_json::json!({"channel": "user_spot_open_orders", "address": OTHER}),
                    );
                    let _ = socket
                        .send(ServerMessage::Text(ack.to_string().into()))
                        .await;
                    let _ = socket.send(ServerMessage::Text(SNAPSHOT.into())).await;
                    let other_page = SNAPSHOT.replace(ADDRESS, OTHER);
                    if scenario != 1 {
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
