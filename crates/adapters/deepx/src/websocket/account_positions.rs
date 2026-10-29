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

//! Read-only initial open-position snapshots for one perpetual market and address scope.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Result, ensure};
pub use deepx_rust_sdk::websocket::account_positions::{
    DeepXWsPerpOpenPositionsFrame, DeepXWsPerpOpenPositionsPage, DeepXWsPerpOpenPositionsRequest,
};
use nautilus_network::transport::Message;

/// Reads one complete initial open-position page from an acknowledged subscription.
///
/// The connection closes after the first page. Position items remain raw observations and do not
/// establish finalized ownership, current exposure, or framework position state.
///
/// # Errors
///
/// Returns an error for connection, acknowledgement, identity, page, or deadline failure.
pub async fn observe_initial_perp_open_positions_page(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsPerpOpenPositionsRequest,
) -> Result<(DeepXWsPerpOpenPositionsPage, u64)> {
    ensure!(
        !request.is_wallet(),
        "DeepX perpetual position single-page observer requires a subaccount request"
    );
    let mut pages =
        observe_initial_perp_open_position_pages(network, proxy_url, timeout, request).await?;
    pages
        .pop()
        .ok_or_else(|| anyhow::anyhow!("DeepX perpetual position snapshot is empty"))
}

/// Reads one complete initial position page for every expected wallet subaccount.
///
/// Pages are sorted by subaccount address. This mutable observation does not prove finalized
/// ownership or exposure.
///
/// # Errors
///
/// Returns an error for a missing, duplicated, foreign, or incomplete page.
pub async fn observe_initial_perp_wallet_open_positions_pages(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsPerpOpenPositionsRequest,
) -> Result<Vec<(DeepXWsPerpOpenPositionsPage, u64)>> {
    ensure!(
        request.is_wallet(),
        "DeepX perpetual position wallet observer requires a wallet request"
    );
    observe_initial_perp_open_position_pages(network, proxy_url, timeout, request).await
}

async fn observe_initial_perp_open_position_pages(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsPerpOpenPositionsRequest,
) -> Result<Vec<(DeepXWsPerpOpenPositionsPage, u64)>> {
    ensure!(
        !timeout.is_zero(),
        "DeepX position timeout must be positive"
    );
    let mut connection =
        super::transport::DeepXWsReadConnection::connect(network, proxy_url, timeout).await?;
    if let Err(e) = connection.send_perp_open_positions_request(request).await {
        let _ = connection.close().await;
        return Err(e);
    }
    let result = tokio::time::timeout(timeout, async {
        let mut acknowledged = false;
        let mut pages: BTreeMap<String, (DeepXWsPerpOpenPositionsPage, u64)> = BTreeMap::new();
        let expected_count = request.expected_page_count();
        loop {
            let message = match connection.receive().await {
                Ok(Some(message)) => message,
                Ok(None) => anyhow::bail!("DeepX position stream closed before its first page"),
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
                anyhow::bail!("unsupported DeepX position stream message");
            };
            match DeepXWsPerpOpenPositionsFrame::parse(std::str::from_utf8(&bytes)?, request)? {
                DeepXWsPerpOpenPositionsFrame::Subscribed => {
                    ensure!(!acknowledged, "duplicate DeepX position acknowledgement");
                    acknowledged = true;
                }
                DeepXWsPerpOpenPositionsFrame::PositionPage { page, timestamp_ms } => {
                    ensure!(acknowledged, "DeepX position page preceded its ack");
                    let address = page.subaccount().to_ascii_lowercase();
                    if let Some((previous, _)) = pages.get(&address) {
                        ensure!(
                            previous.same_contents(&page),
                            "conflicting duplicate DeepX position page"
                        );
                        continue;
                    }
                    pages.insert(address, (page, timestamp_ms));
                    if pages.len() == expected_count {
                        break Ok::<_, anyhow::Error>(pages.into_values().collect());
                    }
                }
                DeepXWsPerpOpenPositionsFrame::Pong => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("DeepX position page deadline exceeded")));
    let close = connection.close().await;
    match result {
        Ok(pages) => {
            close?;
            Ok(pages)
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
    const OTHER: &str = "0x977654069311fa41b88f7901dc65fae9a76fd60b";
    const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
    const ACK: &str = r#"{"type":"subscribed","market":{"type":"perp","id":3,"name":"ETH-USDC"},"subscriptions":[{"channel":"user_perp_positions","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","status":"open","onlyClosed":null}],"message":"Successfully subscribed to 1 channels"}"#;
    const EMPTY: &str = r#"{"type":"data","channel":"user_perp_positions","market":{"type":"perp","id":3,"name":"ETH-USDC"},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","positions":{"hasNext":false,"hasPrev":false,"items":null,"pageNo":1,"pageSize":100,"totalCount":0,"totalPage":0}},"timestamp":1791481423623}"#;

    fn request() -> DeepXWsPerpOpenPositionsRequest {
        DeepXWsPerpOpenPositionsRequest::new(3, ADDRESS).unwrap()
    }

    fn wallet_request() -> DeepXWsPerpOpenPositionsRequest {
        DeepXWsPerpOpenPositionsRequest::for_wallet(
            3,
            WALLET,
            &[ADDRESS.to_string(), OTHER.to_string()],
        )
        .unwrap()
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn observer_requires_ack_before_page(#[case] ack_first: bool) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    let Some(Ok(ServerMessage::Text(_))) = socket.recv().await else {
                        panic!("expected position subscription");
                    };
                    if ack_first {
                        let _ = socket.send(ServerMessage::Text(ACK.into())).await;
                    }
                    let _ = socket.send(ServerMessage::Text(EMPTY.into())).await;
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let result = observe_initial_perp_open_positions_page(
            &network,
            None,
            Duration::from_secs(1),
            &request(),
        )
        .await;
        assert_eq!(result.is_ok(), ack_first);
        server.abort();
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
    #[tokio::test]
    async fn wallet_snapshot_requires_exact_expansion_and_all_pages(#[case] scenario: u8) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    let Some(Ok(ServerMessage::Text(request))) = socket.recv().await else {
                        panic!("expected perpetual wallet position subscription");
                    };
                    let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                    assert_eq!(request["subscriptions"][0]["address"], WALLET);
                    assert_eq!(request["subscriptions"][0]["addressType"], "wallet");
                    assert_eq!(request["subscriptions"][0]["status"], "open");
                    let mut ack: serde_json::Value = serde_json::from_str(ACK).unwrap();
                    if scenario != 4 {
                        ack["subscriptions"]
                            .as_array_mut()
                            .unwrap()
                            .push(serde_json::json!({
                                "channel": "user_perp_positions",
                                "address": OTHER,
                                "status": "open",
                                "onlyClosed": null,
                            }));
                    }
                    if scenario == 5 {
                        ack["subscriptions"][1]["address"] = serde_json::json!(ADDRESS);
                    } else if scenario == 6 {
                        ack["subscriptions"][1]["address"] =
                            serde_json::json!("0x0000000000000000000000000000000000000000");
                    } else if scenario == 7 {
                        ack["subscriptions"][1]["status"] = serde_json::json!("closed");
                    }
                    let _ = socket
                        .send(ServerMessage::Text(ack.to_string().into()))
                        .await;
                    let _ = socket.send(ServerMessage::Text(EMPTY.into())).await;
                    let other_page = EMPTY.replace(ADDRESS, OTHER);
                    if scenario == 2 {
                        let _ = socket.send(ServerMessage::Text(EMPTY.into())).await;
                        let _ = socket.send(ServerMessage::Text(other_page.into())).await;
                    } else if scenario == 8 {
                        let conflict = EMPTY.replace("\"pageSize\":100", "\"pageSize\":50");
                        let _ = socket.send(ServerMessage::Text(conflict.into())).await;
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
        let result = observe_initial_perp_wallet_open_positions_pages(
            &network,
            None,
            Duration::from_secs(2),
            &wallet_request(),
        )
        .await;
        if scenario == 0 || scenario == 2 {
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
            observe_initial_perp_open_positions_page(
                &network,
                None,
                Duration::from_secs(1),
                &wallet_request(),
            )
            .await
            .is_err()
        );
        assert!(
            observe_initial_perp_wallet_open_positions_pages(
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
