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

use std::{collections::HashSet, time::Duration};

use anyhow::{Context, Result, ensure};
pub use deepx_rust_sdk::websocket::account_trade_history::{
    DeepXWsPerpTradeHistoryBatch, DeepXWsPerpTradeHistoryFrame, DeepXWsPerpTradeHistoryItem,
    DeepXWsPerpTradeHistoryRequest,
};
use nautilus_network::transport::Message;
#[cfg(test)]
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    http::{DeepXPerpAccountTradeRecord, DeepXPerpAccountTradesRequest},
    transaction::{
        DeepXFinalizedPerpTrade, DeepXPerpTradeMatchError, match_finalized_perp_account_trade,
    },
};

/// Failures matching one incremental WebSocket batch to validated REST account trades.
#[derive(Debug, Error)]
pub enum DeepXWsPerpTradeHistoryMatchError {
    /// The REST request did not bind the exact WebSocket subaccount and market scope.
    #[error("DeepX trade-history REST request conflicts with WebSocket scope")]
    RequestScope,
    /// The WebSocket timestamp could not be compared exactly.
    #[error("invalid DeepX trade-history timestamp")]
    InvalidTimestamp,
    /// No supplied REST record agreed with one WebSocket item.
    #[error("DeepX incremental trade has no matching REST account record")]
    NotFound,
    /// Two supplied REST records agree with one WebSocket item.
    #[error("DeepX incremental trade matches multiple REST account records")]
    Ambiguous,
    /// Two WebSocket items would consume the same REST trade identity.
    #[error("DeepX incremental trade batch reuses one REST trade identity")]
    DuplicateTrade,
    /// The WebSocket batch subaccount cannot be bound to the supplied chain account.
    #[error("DeepX trade-history subaccount conflicts with finalized account")]
    FinalizedAccountScope,
    /// One REST record could not be matched to the supplied finalized event evidence.
    #[error(transparent)]
    Finalized(#[from] DeepXPerpTradeMatchError),
}

/// Independently validated account and market units for finalized trade matching.
#[derive(Clone, Copy, Debug)]
pub struct DeepXWsPerpFinalizedTradeScope<'a> {
    /// AccountId20 bound to the WebSocket batch and REST request.
    pub subaccount: [u8; 20],
    /// Base asset decimal places used by runtime trade size.
    pub base_decimal: u8,
    /// Quote asset decimal places used by runtime trade fee.
    pub quote_decimal: u8,
    /// Quote asset symbol carried by the runtime fee asset.
    pub quote_symbol: &'a str,
}

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
    use DeepXWsPerpTradeHistoryMatchError as Error;

    if request.validate().is_err()
        || !request.subaccount.eq_ignore_ascii_case(batch.subaccount())
        || request
            .market_id
            .is_some_and(|market_id| batch.items().iter().any(|item| item.market_id != market_id))
        || request
            .order_id
            .as_ref()
            .is_some_and(|order_id| batch.items().iter().any(|item| item.order_id != *order_id))
        || request
            .is_long
            .is_some_and(|is_long| batch.items().iter().any(|item| item.is_long != is_long))
    {
        return Err(Error::RequestScope);
    }
    let mut matched = Vec::with_capacity(batch.items().len());
    let mut trade_ids = HashSet::with_capacity(batch.items().len());
    for item in batch.items() {
        let timestamp = item
            .created_at
            .parse::<jiff::Timestamp>()
            .map_err(|_| Error::InvalidTimestamp)?;
        let timestamp_ms =
            u64::try_from(timestamp.as_millisecond()).map_err(|_| Error::InvalidTimestamp)?;
        if request.start_ms.is_some_and(|start| timestamp_ms < start)
            || request.end_ms.is_some_and(|end| timestamp_ms > end)
        {
            return Err(Error::RequestScope);
        }
        let mut candidates = records.iter().filter(|record| {
            record.id != 0
                && item.id.is_none_or(|id| id == record.id)
                && record.market_id == item.market_id
                && record.order_id == item.order_id
                && record.is_long == item.is_long
                && record.taker == item.taker
                && record.price == item.price
                && record.size == item.size
                && record.leverage == item.leverage
                && record.filled_direction == item.filled_direction
                && record.fee == item.fee
                && item
                    .fee_asset
                    .as_ref()
                    .is_none_or(|asset| asset.eq_ignore_ascii_case(&record.fee_asset))
                && record
                    .created_at
                    .parse::<jiff::Timestamp>()
                    .is_ok_and(|value| value == timestamp)
        });
        let record = candidates.next().ok_or(Error::NotFound)?;
        if candidates.next().is_some() {
            return Err(Error::Ambiguous);
        }
        if !trade_ids.insert(record.id) {
            return Err(Error::DuplicateTrade);
        }
        matched.push(record);
    }
    Ok(matched)
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
    use DeepXWsPerpTradeHistoryMatchError as Error;

    let encoded_subaccount = format!("0x{}", nautilus_core::hex::encode(scope.subaccount));
    if !batch.subaccount().eq_ignore_ascii_case(&encoded_subaccount) {
        return Err(Error::FinalizedAccountScope);
    }
    let records = match_perp_trade_history_batch_to_rest(batch, request, records)?;
    records
        .into_iter()
        .map(|record| {
            let trade = match_finalized_perp_account_trade(
                trades,
                record,
                scope.subaccount,
                scope.base_decimal,
                scope.quote_decimal,
                scope.quote_symbol,
            )?;
            Ok((record, trade))
        })
        .collect()
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
    fn wallet_ack_matches_exact_directory_and_market() {
        let request = DeepXWsPerpTradeHistoryRequest::for_wallet(
            3,
            WALLET,
            &[ADDRESS.to_string(), OTHER.to_string()],
        )
        .unwrap();
        let encoded: serde_json::Value = serde_json::from_str(&request.to_text().unwrap()).unwrap();
        assert_eq!(
            encoded["subscriptions"][0]["channel"],
            "user_perp_trade_history"
        );
        assert_eq!(encoded["subscriptions"][0]["addressType"], "wallet");
        assert!(request.validate_ack(ACK).is_ok());
        assert!(request.validate_ack(&ACK.replace(OTHER, ADDRESS)).is_err());
        assert!(request.validate_ack(&ACK.replace(OTHER, WALLET)).is_err());
        assert!(
            request
                .validate_ack(&ACK.replace("\"id\":3", "\"id\":4"))
                .is_err()
        );
        assert!(
            request
                .validate_ack(&ACK.replace("user_perp_trade_history", "user_perp_open_orders"))
                .is_err()
        );
        assert!(
            request
                .validate_ack(&ACK.replace("subscribed", "data"))
                .is_err()
        );
    }

    #[rstest]
    fn trade_history_request_rejects_invalid_scope() {
        assert!(DeepXWsPerpTradeHistoryRequest::new(0, ADDRESS).is_err());
        assert!(DeepXWsPerpTradeHistoryRequest::new(3, "invalid").is_err());
        assert!(DeepXWsPerpTradeHistoryRequest::for_wallet(3, WALLET, &[]).is_err());
        assert!(
            DeepXWsPerpTradeHistoryRequest::for_wallet(
                3,
                WALLET,
                &[ADDRESS.to_string(), ADDRESS.to_string()],
            )
            .is_err()
        );
    }

    #[rstest]
    fn documented_incremental_batch_retains_raw_numbers_and_binds_identity() {
        let request = DeepXWsPerpTradeHistoryRequest::for_wallet(
            3,
            WALLET,
            &[ADDRESS.to_string(), OTHER.to_string()],
        )
        .unwrap();
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsPerpTradeHistoryFrame::parse(DATA, &request).unwrap()
        else {
            panic!("expected raw trade batch");
        };
        assert_eq!(batch.subaccount(), ADDRESS);
        assert_eq!(batch.timestamp_ms(), 1_791_475_653_766);
        assert_eq!(batch.items().len(), 1);
        assert_eq!(batch.items()[0].price, Decimal::new(220_976, 2));
        assert_eq!(batch.items()[0].id, None);
        assert_eq!(batch.items()[0].fee_asset, None);
        assert!(batch.items()[0].raw().get().contains("2209.7600"));
        let with_id = DATA.replace(
            "\"marketId\":3",
            "\"id\":187071212000014,\"marketId\":3,\"feeAsset\":\"USDC\"",
        );
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(with_id) =
            DeepXWsPerpTradeHistoryFrame::parse(&with_id, &request).unwrap()
        else {
            panic!("expected trade batch with venue ID");
        };
        assert_eq!(with_id.items()[0].id, Some(187_071_212_000_014));
        assert_eq!(with_id.items()[0].fee_asset.as_deref(), Some("USDC"));
        let first_generated = DATA.replace("\"orderId\":\"87561\"", "\"orderId\":\"0\"");
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(first_generated) =
            DeepXWsPerpTradeHistoryFrame::parse(&first_generated, &request).unwrap()
        else {
            panic!("expected zero-ID trade batch");
        };
        assert_eq!(first_generated.items()[0].order_id, "0");
        for changed in [
            DATA.replace(ADDRESS, WALLET),
            DATA.replace("\"id\":3", "\"id\":4"),
            DATA.replace("\"marketId\":3", "\"marketId\":4"),
            DATA.replace("\"orderId\":\"87561\"", "\"orderId\":\"00\""),
            DATA.replace("\"hasNext\":false", "\"hasNext\":true"),
            DATA.replace("\"timestamp\":1791475653766", "\"timestamp\":0"),
            DATA.replace("\"size\":0.01", "\"size\":-0.01"),
            DATA.replace("\"fee\":0.02", "\"fee\":\"invalid\""),
            DATA.replace("\"taker\":\"Buyer\"", "\"taker\":\"Other\""),
            DATA.replace(
                "\"createdAt\":\"2026-02-06T14:52:25.613Z\"",
                "\"createdAt\":\"invalid\"",
            ),
        ] {
            assert!(DeepXWsPerpTradeHistoryFrame::parse(&changed, &request).is_err());
        }
    }

    #[rstest]
    fn incremental_batch_matches_only_unique_exact_rest_trades() {
        let ws_request = DeepXWsPerpTradeHistoryRequest::new(3, ADDRESS).unwrap();
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(batch) =
            DeepXWsPerpTradeHistoryFrame::parse(DATA, &ws_request).unwrap()
        else {
            panic!("expected trade batch");
        };
        let mut request = DeepXPerpAccountTradesRequest {
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
        let record = DeepXPerpAccountTradeRecord(
            deepx_rust_sdk::rest::models::DeepXPerpAccountTradeRecord {
                id: 187_071_212_000_014,
                market_id: 3,
                order_id: "87561".to_string(),
                is_long: true,
                taker: "Buyer".to_string(),
                price: Decimal::new(220_976, 2),
                size: Decimal::new(1, 2),
                leverage: Decimal::TEN,
                created_at: "2026-02-06T14:52:25.613Z".to_string(),
                filled_direction: "Long".to_string(),
                fee: Decimal::new(2, 2),
                fee_asset: String::new(),
            },
        );
        assert_eq!(
            match_perp_trade_history_batch_to_rest(&batch, &request, std::slice::from_ref(&record))
                .unwrap()[0]
                .id,
            record.id,
        );
        let created_ms = u64::try_from(
            record
                .created_at
                .parse::<jiff::Timestamp>()
                .unwrap()
                .as_millisecond(),
        )
        .unwrap();
        request.start_ms = Some(created_ms + 1);
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&batch, &request, std::slice::from_ref(&record)),
            Err(DeepXWsPerpTradeHistoryMatchError::RequestScope),
        ));
        request.start_ms = Some(created_ms);
        request.end_ms = Some(created_ms);
        assert_eq!(
            match_perp_trade_history_batch_to_rest(&batch, &request, std::slice::from_ref(&record))
                .unwrap()[0]
                .id,
            record.id,
        );
        request.start_ms = None;
        request.end_ms = Some(created_ms - 1);
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&batch, &request, std::slice::from_ref(&record)),
            Err(DeepXWsPerpTradeHistoryMatchError::RequestScope),
        ));
        request.start_ms = Some(created_ms);
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&batch, &request, std::slice::from_ref(&record)),
            Err(DeepXWsPerpTradeHistoryMatchError::RequestScope),
        ));
        request.start_ms = None;
        request.end_ms = None;
        let mut with_fee_asset = record.clone();
        with_fee_asset.fee_asset = "USDC".to_string();
        let with_id = DATA.replace(
            "\"marketId\":3",
            "\"id\":187071212000014,\"marketId\":3,\"feeAsset\":\"USDC\"",
        );
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(with_id_batch) =
            DeepXWsPerpTradeHistoryFrame::parse(&with_id, &ws_request).unwrap()
        else {
            panic!("expected trade batch with venue ID and fee asset");
        };
        assert_eq!(
            match_perp_trade_history_batch_to_rest(
                &with_id_batch,
                &request,
                &[with_fee_asset.clone()],
            )
            .unwrap()[0]
                .id,
            record.id,
        );
        let empty_fee_asset = with_id.replace("\"feeAsset\":\"USDC\"", "\"feeAsset\":\"\"");
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(empty_fee_asset_batch) =
            DeepXWsPerpTradeHistoryFrame::parse(&empty_fee_asset, &ws_request).unwrap()
        else {
            panic!("expected trade batch with empty fee asset");
        };
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(
                &empty_fee_asset_batch,
                &request,
                &[with_fee_asset.clone()],
            ),
            Err(DeepXWsPerpTradeHistoryMatchError::NotFound),
        ));
        with_fee_asset.id += 1;
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&with_id_batch, &request, &[with_fee_asset]),
            Err(DeepXWsPerpTradeHistoryMatchError::NotFound),
        ));
        let mut changed_fee = record.clone();
        changed_fee.fee += Decimal::new(1, 6);
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&batch, &request, &[changed_fee]),
            Err(DeepXWsPerpTradeHistoryMatchError::NotFound),
        ));
        let mut different_id = record.clone();
        different_id.id += 1;
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(
                &batch,
                &request,
                &[record.clone(), different_id]
            ),
            Err(DeepXWsPerpTradeHistoryMatchError::Ambiguous),
        ));
        let mut repeated: serde_json::Value = serde_json::from_str(DATA).unwrap();
        let item = repeated["data"]["trades"]["items"][0].clone();
        repeated["data"]["trades"]["items"]
            .as_array_mut()
            .unwrap()
            .push(item);
        let DeepXWsPerpTradeHistoryFrame::TradeBatch(repeated) =
            DeepXWsPerpTradeHistoryFrame::parse(&repeated.to_string(), &ws_request).unwrap()
        else {
            panic!("expected repeated trade batch");
        };
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(
                &repeated,
                &request,
                std::slice::from_ref(&record)
            ),
            Err(DeepXWsPerpTradeHistoryMatchError::DuplicateTrade),
        ));
        request.subaccount = OTHER.to_string();
        assert!(matches!(
            match_perp_trade_history_batch_to_rest(&batch, &request, &[record]),
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
