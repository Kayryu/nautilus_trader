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

//! Read-only perpetual account trade subscription acknowledgement.

use std::time::Duration;

use anyhow::{Context, Result, ensure};
pub use deepx_rust_sdk::websocket::account_trade_history::{
    DeepXWsPerpFinalizedTradeScope, DeepXWsPerpTradeHistoryBatch, DeepXWsPerpTradeHistoryFrame,
    DeepXWsPerpTradeHistoryItem, DeepXWsPerpTradeHistoryMatchError, DeepXWsPerpTradeHistoryRequest,
};
use deepx_rust_sdk::websocket::account_trade_history::{
    match_perp_trade_history_batch_to_finalized as sdk_match_finalized,
    match_perp_trade_history_batch_to_rest as sdk_match_rest,
};
use nautilus_network::transport::Message;

use crate::{
    http::{DeepXPerpAccountTradeRecord, DeepXPerpAccountTradesRequest},
    transaction::DeepXFinalizedPerpTrade,
};

/// Matches every incremental item to one REST account trade without partial results.
///
/// The caller supplies records obtained and validated with the exact request. The supplied slice
/// is not a block-pinned or necessarily complete history. A match does not prove ownership beyond
/// the REST request, chain finality, replay continuity, or permission to dispatch a framework fill.
///
/// # Errors
///
/// Returns an error for scope conflict, invalid timestamps, absent or ambiguous records, or a
/// duplicate matched venue trade ID within the batch.
pub fn match_perp_trade_history_batch_to_rest<'a>(
    batch: &DeepXWsPerpTradeHistoryBatch,
    request: &DeepXPerpAccountTradesRequest,
    records: &'a [DeepXPerpAccountTradeRecord],
) -> Result<Vec<&'a DeepXPerpAccountTradeRecord>, DeepXWsPerpTradeHistoryMatchError> {
    sdk_match_rest(batch, request, records).map_err(Into::into)
}

/// Reconciles every WebSocket item through REST to a supplied finalized trade event.
///
/// The caller independently validates the REST request response and canonical finality of every
/// supplied event block, along with the account's market scales and quote asset. This pure match
/// does not prove stream continuity, REST historical completeness, or permission to emit fills.
///
/// # Errors
///
/// Returns an error if the account scope conflicts or any item fails either exact match stage.
pub fn match_perp_trade_history_batch_to_finalized<'a, 'b>(
    batch: &DeepXWsPerpTradeHistoryBatch,
    request: &DeepXPerpAccountTradesRequest,
    records: &'a [DeepXPerpAccountTradeRecord],
    trades: &'b [DeepXFinalizedPerpTrade],
    scope: DeepXWsPerpFinalizedTradeScope<'_>,
) -> Result<
    Vec<(&'a DeepXPerpAccountTradeRecord, &'b DeepXFinalizedPerpTrade)>,
    DeepXWsPerpTradeHistoryMatchError,
> {
    sdk_match_finalized(batch, request, records, trades, scope)
}

/// Confirms one read-only subscription and closes before receiving business data.
///
/// This proves only that the endpoint acknowledged the exact requested market and addresses.
/// It does not authenticate a private execution stream or decode trade data.
///
/// # Errors
///
/// Returns an error for connection, send, deadline, malformed frame, or foreign acknowledgement.
pub async fn observe_perp_trade_history_ack(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsPerpTradeHistoryRequest,
) -> Result<()> {
    ensure!(
        !timeout.is_zero(),
        "DeepX trade-history timeout must be positive"
    );
    let mut connection =
        super::transport::DeepXWsReadConnection::connect(network, proxy_url, timeout).await?;
    connection.send_perp_trade_history_request(request).await?;
    let result = tokio::time::timeout(timeout, async {
        loop {
            let Some(message) = connection.receive().await? else {
                anyhow::bail!("DeepX trade-history stream closed before acknowledgement");
            };
            match message {
                Message::Text(bytes) => {
                    match DeepXWsPerpTradeHistoryFrame::parse(
                        std::str::from_utf8(&bytes)?,
                        request,
                    )? {
                        DeepXWsPerpTradeHistoryFrame::Subscribed => {
                            break Ok::<_, anyhow::Error>(());
                        }
                        DeepXWsPerpTradeHistoryFrame::TradeBatch(_) => {
                            anyhow::bail!("DeepX trade-history data preceded its acknowledgement")
                        }
                        DeepXWsPerpTradeHistoryFrame::Pong => {}
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => anyhow::bail!("unsupported DeepX trade-history stream message"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        Err(anyhow::anyhow!(
            "DeepX trade-history acknowledgement deadline exceeded"
        ))
    });
    let close = connection.close().await;
    result.and(close)
}

/// Collects bounded, address-bound incremental trade batches after subscription acknowledgement.
///
/// The observation window begins after the subscription is sent. An empty result means no trade
/// batch arrived during that window; it does not establish an empty account history. Batches are
/// mutable WebSocket observations and do not authorize fill delivery.
///
/// # Errors
///
/// Returns an error for invalid or exceeded bounds, missing or duplicate acknowledgement,
/// malformed or foreign frames, transport failure, or closure before the window ends.
pub async fn observe_perp_trade_history_batches(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    window: Duration,
    max_batches: usize,
    max_items: usize,
    request: &DeepXWsPerpTradeHistoryRequest,
) -> Result<Vec<DeepXWsPerpTradeHistoryBatch>> {
    ensure!(
        !window.is_zero(),
        "DeepX trade-history window must be positive"
    );
    ensure!(
        max_batches > 0,
        "DeepX trade-history batch limit must be positive"
    );
    ensure!(
        max_items > 0,
        "DeepX trade-history item limit must be positive"
    );
    let mut connection =
        super::transport::DeepXWsReadConnection::connect(network, proxy_url, window).await?;
    connection.send_perp_trade_history_request(request).await?;
    let deadline = tokio::time::Instant::now() + window;
    let result = async {
        let mut acknowledged = false;
        let mut batches = Vec::new();
        let mut item_count = 0_usize;
        loop {
            let message = match tokio::time::timeout_at(deadline, connection.receive()).await {
                Ok(Ok(Some(message))) => message,
                Ok(Ok(None)) => {
                    anyhow::bail!("DeepX trade-history stream closed during observation")
                }
                Ok(Err(e))
                    if matches!(
                        e.downcast_ref::<super::DeepXWsError>(),
                        Some(super::DeepXWsError::ReceiveTimeout)
                    ) =>
                {
                    continue;
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    ensure!(
                        acknowledged,
                        "DeepX trade-history acknowledgement deadline exceeded"
                    );
                    return Ok(batches);
                }
            };
            let Message::Text(bytes) = message else {
                if matches!(message, Message::Ping(_) | Message::Pong(_)) {
                    continue;
                }
                anyhow::bail!("unsupported DeepX trade-history stream message");
            };
            match DeepXWsPerpTradeHistoryFrame::parse(std::str::from_utf8(&bytes)?, request)? {
                DeepXWsPerpTradeHistoryFrame::Subscribed => {
                    ensure!(
                        !acknowledged,
                        "duplicate DeepX trade-history acknowledgement"
                    );
                    acknowledged = true;
                }
                DeepXWsPerpTradeHistoryFrame::TradeBatch(batch) => {
                    ensure!(
                        acknowledged,
                        "DeepX trade-history data preceded its acknowledgement"
                    );
                    item_count = item_count
                        .checked_add(batch.items().len())
                        .context("DeepX trade-history item count overflowed")?;
                    ensure!(
                        item_count <= max_items,
                        "DeepX trade-history item limit exceeded"
                    );
                    batches.push(batch);
                    if batches.len() == max_batches {
                        return Ok(batches);
                    }
                }
                DeepXWsPerpTradeHistoryFrame::Pong => {}
            }
        }
    }
    .await;
    let close = connection.close().await;
    result.and_then(|batches| close.map(|()| batches))
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
    use crate::http::DeepXAccountSortOrder;

    const ADDRESS: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
    const OTHER: &str = "0x977654069311fa41b88f7901dc65fae9a76fd60b";
    const WALLET: &str = "0x781ed35b167068c93dfadab41dfb680edaca4e50";
    const ACK: &str = r#"{"type":"subscribed","market":{"type":"perp","id":3,"name":"ETH-USDC"},"subscriptions":[{"channel":"user_perp_trade_history","address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"},{"channel":"user_perp_trade_history","address":"0x977654069311fa41b88f7901dc65fae9a76fd60b"}],"message":"Successfully subscribed to 2 channels"}"#;
    const DATA: &str = r#"{"type":"data","channel":"user_perp_trade_history","market":{"type":"perp","id":3},"data":{"address":"0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc","trades":{"items":[{"marketId":3,"orderId":"87561","isLong":true,"taker":"Buyer","price":2209.7600,"size":0.01,"leverage":10,"createdAt":"2026-02-06T14:52:25.613Z","filledDirection":"Long","fee":0.02}],"nextCursor":null,"hasNext":false}},"timestamp":1791475653766}"#;

    #[rstest]
    fn rest_match_preserves_adapter_record_identity_and_error() {
        let ws_request = DeepXWsPerpTradeHistoryRequest::new(3, ADDRESS).unwrap();
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsPerpTradeHistoryFrame::parse(DATA, &ws_request).unwrap()
        else {
            panic!("expected trade batch");
        };
        let request = DeepXPerpAccountTradesRequest {
            subaccount: ADDRESS.to_string(),
            order_id: None,
            market_id: Some(3),
            is_long: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: None,
            end_ms: None,
            page_size: Some(100),
        };
        let record: DeepXPerpAccountTradeRecord = serde_json::from_str(
            r#"{"id":187071212000014,"marketId":3,"orderId":"87561","isLong":true,"taker":"Buyer","price":2209.7600,"size":0.01,"leverage":10,"createdAt":"2026-02-06T14:52:25.613Z","filledDirection":"Long","fee":0.02,"feeAsset":""}"#,
        )
        .unwrap();
        let matched =
            match_perp_trade_history_batch_to_rest(&batch, &request, std::slice::from_ref(&record))
                .unwrap();
        assert!(std::ptr::eq(matched[0], &record));

        let foreign_request = DeepXPerpAccountTradesRequest {
            subaccount: OTHER.to_string(),
            ..request
        };
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(
                &batch,
                &foreign_request,
                std::slice::from_ref(&record),
            ),
            Err(DeepXWsPerpTradeHistoryMatchError::RequestScope),
        ));
    }

    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[tokio::test]
    async fn bounded_observer_requires_ack_and_collects_only_later_data(#[case] scenario: u8) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    let Some(Ok(ServerMessage::Text(_))) = socket.recv().await else {
                        panic!("expected perpetual trade-history subscription");
                    };
                    match scenario {
                        1 => {
                            socket.send(ServerMessage::Text(ACK.into())).await.unwrap();
                            socket.send(ServerMessage::Text(DATA.into())).await.unwrap();
                        }
                        2 => {
                            socket.send(ServerMessage::Text(ACK.into())).await.unwrap();
                        }
                        3 => {
                            socket.send(ServerMessage::Text(DATA.into())).await.unwrap();
                        }
                        5 => {
                            socket.send(ServerMessage::Text(ACK.into())).await.unwrap();
                            socket.send(ServerMessage::Text(DATA.into())).await.unwrap();
                            socket.send(ServerMessage::Text(DATA.into())).await.unwrap();
                        }
                        _ => {
                            socket.send(ServerMessage::Text(ACK.into())).await.unwrap();
                            socket.send(ServerMessage::Text(ACK.into())).await.unwrap();
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let request = DeepXWsPerpTradeHistoryRequest::for_wallet(
            3,
            WALLET,
            &[ADDRESS.to_string(), OTHER.to_string()],
        )
        .unwrap();
        let result = observe_perp_trade_history_batches(
            &network,
            None,
            Duration::from_millis(500),
            if scenario == 5 { 2 } else { 1 },
            1,
            &request,
        )
        .await;
        match scenario {
            1 => assert_eq!(result.unwrap()[0].items()[0].order_id, "87561"),
            2 => assert!(result.unwrap().is_empty()),
            _ => assert!(result.is_err()),
        }
        server.abort();
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[tokio::test]
    async fn observer_requires_exact_ack_before_data(#[case] scenario: u8) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    let Some(Ok(ServerMessage::Text(request))) = socket.recv().await else {
                        panic!("expected perpetual trade-history subscription");
                    };
                    let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                    assert_eq!(
                        request["subscriptions"][0]["channel"],
                        "user_perp_trade_history"
                    );
                    assert_eq!(request["subscriptions"][0]["addressType"], "wallet");
                    let frame = match scenario {
                        0 => ACK.to_string(),
                        1 => ACK.replace(OTHER, WALLET),
                        _ => ACK.replace("subscribed", "data"),
                    };
                    let _ = socket.send(ServerMessage::Text(frame.into())).await;
                    let _ = socket.recv().await;
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let request = DeepXWsPerpTradeHistoryRequest::for_wallet(
            3,
            WALLET,
            &[ADDRESS.to_string(), OTHER.to_string()],
        )
        .unwrap();
        let result =
            observe_perp_trade_history_ack(&network, None, Duration::from_secs(2), &request).await;
        assert_eq!(result.is_ok(), scenario == 0);
        server.abort();
    }
}
