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

//! Read-only, address-bound Spot trade-history observations.

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use deepx_rust_sdk::websocket::account_spot_trade_history::{
    DeepXWsMappedSpotTradeHistoryFrame as SdkSpotTradeHistoryFrame,
    DeepXWsSpotTradeHistoryBatch as SdkSpotTradeHistoryBatch,
    DeepXWsSpotTradeHistoryItem as SdkSpotTradeHistoryItem,
    match_spot_trade_history_batch_to_finalized as sdk_match_finalized,
    match_spot_trade_history_batch_to_rest as sdk_match_rest,
};
pub use deepx_rust_sdk::websocket::account_spot_trade_history::{
    DeepXWsSpotFinalizedTradeScope, DeepXWsSpotTradeHistoryMatchError,
    DeepXWsSpotTradeHistoryRequest,
};
use nautilus_network::transport::Message;

use super::transport::DeepXWsReadConnection;
use crate::{
    config::DeepXNetworkConfig,
    http::{DeepXSpotAccountTradeRecord, DeepXSpotAccountTradesRequest},
    transaction::DeepXFinalizedSpotTrade,
};

/// One unconfirmed Spot trade with its original JSON and Python-facing record.
pub type DeepXWsSpotTradeHistoryItem = SdkSpotTradeHistoryItem<DeepXSpotAccountTradeRecord>;

/// One incremental Spot trade batch with Python-facing records.
pub type DeepXWsSpotTradeHistoryBatch = SdkSpotTradeHistoryBatch<DeepXSpotAccountTradeRecord>;

/// One frame admitted for an exact Spot trade-history subscription.
pub type DeepXWsSpotTradeHistoryFrame = SdkSpotTradeHistoryFrame<DeepXSpotAccountTradeRecord>;

/// Matches every incremental Spot trade to one validated REST account record atomically.
///
/// The caller supplies records validated under the exact REST request. REST does not echo the
/// account on each record and is not block-pinned; a match is only mutable candidate evidence.
/// It does not prove chain finality, stream continuity, or permission to dispatch a fill.
///
/// # Errors
///
/// Returns an error for scope conflict, invalid timestamps, missing or ambiguous records, or
/// duplicate matched trade identities.
pub fn match_spot_trade_history_batch_to_rest<'a>(
    batch: &DeepXWsSpotTradeHistoryBatch,
    subscription: &DeepXWsSpotTradeHistoryRequest,
    request: &DeepXSpotAccountTradesRequest,
    records: &'a [DeepXSpotAccountTradeRecord],
) -> Result<Vec<&'a DeepXSpotAccountTradeRecord>, DeepXWsSpotTradeHistoryMatchError> {
    sdk_match_rest(batch, subscription, request, records).map_err(Into::into)
}

/// Matches every Spot WebSocket item through REST to supplied finalized chain events.
///
/// The caller validates the REST page, market metadata, and canonical finality of all event
/// blocks independently. This pure match does not prove stream continuity or historical REST
/// completeness and does not authorize framework fill delivery.
///
/// # Errors
///
/// Returns an error if account scope conflicts or either exact match stage fails.
pub fn match_spot_trade_history_batch_to_finalized<'a, 'b>(
    batch: &DeepXWsSpotTradeHistoryBatch,
    subscription: &DeepXWsSpotTradeHistoryRequest,
    request: &DeepXSpotAccountTradesRequest,
    records: &'a [DeepXSpotAccountTradeRecord],
    trades: &'b [DeepXFinalizedSpotTrade],
    scope: DeepXWsSpotFinalizedTradeScope<'_>,
) -> Result<
    Vec<(&'a DeepXSpotAccountTradeRecord, &'b DeepXFinalizedSpotTrade)>,
    DeepXWsSpotTradeHistoryMatchError,
> {
    sdk_match_finalized(batch, subscription, request, records, trades, scope)
}

/// Observes bounded incremental batches after a read-only subscription acknowledgement.
///
/// An empty result only means no batch arrived during this window. This mutable stream does not
/// establish complete history, chain finality, or permission to dispatch framework fills.
///
/// # Errors
///
/// Returns an error for invalid bounds, malformed or foreign frames, missing acknowledgement,
/// exceeded limits, transport failure, or early stream closure.
pub async fn observe_spot_trade_history_batches(
    network: &DeepXNetworkConfig,
    proxy_url: Option<&str>,
    window: Duration,
    max_batches: usize,
    max_items: usize,
    request: &DeepXWsSpotTradeHistoryRequest,
) -> Result<Vec<DeepXWsSpotTradeHistoryBatch>> {
    ensure!(
        !window.is_zero(),
        "DeepX Spot trade-history window must be positive"
    );
    ensure!(
        max_batches > 0,
        "DeepX Spot trade-history batch limit must be positive"
    );
    ensure!(
        max_items > 0,
        "DeepX Spot trade-history item limit must be positive"
    );
    let mut connection = DeepXWsReadConnection::connect(network, proxy_url, window).await?;
    connection.send_spot_trade_history_request(request).await?;
    let deadline = tokio::time::Instant::now() + window;
    let result = async {
        let mut acknowledged = false;
        let mut batches = Vec::new();
        let mut item_count = 0_usize;
        loop {
            let message = match tokio::time::timeout_at(deadline, connection.receive()).await {
                Ok(Ok(Some(message))) => message,
                Ok(Ok(None)) => anyhow::bail!("DeepX Spot trade-history stream closed"),
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
                        "DeepX Spot trade-history acknowledgement timed out"
                    );
                    return Ok(batches);
                }
            };
            let Message::Text(bytes) = message else {
                if matches!(message, Message::Ping(_) | Message::Pong(_)) {
                    continue;
                }
                anyhow::bail!("unsupported DeepX Spot trade-history stream message");
            };
            match DeepXWsSpotTradeHistoryFrame::parse(std::str::from_utf8(&bytes)?, request)? {
                DeepXWsSpotTradeHistoryFrame::Subscribed => {
                    ensure!(
                        !acknowledged,
                        "duplicate DeepX Spot trade-history acknowledgement"
                    );
                    acknowledged = true;
                }
                DeepXWsSpotTradeHistoryFrame::TradeBatch(batch) => {
                    ensure!(
                        acknowledged,
                        "DeepX Spot trade data preceded acknowledgement"
                    );
                    item_count = item_count
                        .checked_add(batch.items().len())
                        .context("DeepX Spot trade-history item count overflowed")?;
                    ensure!(
                        item_count <= max_items,
                        "DeepX Spot trade-history item limit exceeded"
                    );
                    batches.push(batch);
                    if batches.len() == max_batches {
                        return Ok(batches);
                    }
                }
                DeepXWsSpotTradeHistoryFrame::Pong => {}
            }
        }
    }
    .await;
    let close = connection.close().await;
    result.and_then(|batches| close.map(|()| batches))
}
