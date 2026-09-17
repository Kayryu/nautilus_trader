// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live read-only public subscription acknowledgement and raw data verification.

use anyhow::Context;

use nautilus_deepx::websocket::book::DeepXWsBookStream;
use nautilus_deepx::websocket::trades::DeepXWsTradeStream;
use nautilus_deepx::{
    config::DeepXNetworkConfig,
    websocket::public::{DeepXWsPublicChannel, DeepXWsPublicConnection, DeepXWsPublicFrame},
};
use std::{num::NonZeroUsize, time::Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut connection = DeepXWsPublicConnection::connect(
        &DeepXNetworkConfig::default(),
        None,
        Duration::from_secs(10),
        NonZeroUsize::new(32).unwrap(),
    )
    .await?;
    connection
        .subscribe(
            2,
            vec![
                DeepXWsPublicChannel::Trades,
                DeepXWsPublicChannel::Orderbook,
            ],
        )
        .await?;
    println!("DeepX BTC public trades/orderbook acknowledgement verified");
    let mut trades = false;
    let mut book = false;
    let mut book_updates = 0;
    let mut book_stream = DeepXWsBookStream::new(2, NonZeroUsize::new(1024).unwrap());
    let mut initial_trade_page = false;
    let mut trade_stream = DeepXWsTradeStream::new(2, NonZeroUsize::new(1024).unwrap());
    tokio::time::timeout(Duration::from_secs(15), async {
        while !trades || !book {
            let frame = connection.next_frame().await?;
            if let DeepXWsPublicFrame::Data {
                market,
                channel,
                data,
                ..
            } = &frame
            {
                if *channel == DeepXWsPublicChannel::Trades {
                    let executions = trade_stream.ingest(&frame)?;
                    if !initial_trade_page {
                        anyhow::ensure!(
                            executions.is_empty(),
                            "initial history was emitted as live trades"
                        );
                        initial_trade_page = true;
                        println!("DeepX initial trade history seeded without live delivery");
                    } else {
                        println!("DeepX novel public executions={}", executions.len());
                        trades |= !executions.is_empty();
                    }
                } else if *channel == DeepXWsPublicChannel::Orderbook {
                    book_stream.ingest(&frame)?;
                    book_updates += 1;
                    let snapshot = book_stream.snapshot().unwrap();
                    println!(
                        "DeepX market={} orderbook raw_payload_bytes={} sequence={} bids={} asks={}",
                        market.id,
                        data.get().len(),
                        snapshot.last_update_id,
                        snapshot.bids.len(),
                        snapshot.asks.len(),
                    );
                    book = book_updates >= 2;
                }
            }
        }
        anyhow::Ok(())
    })
    .await??;
    connection
        .close()
        .await
        .context("failed to close the original public connection")?;
    if std::env::args().any(|arg| arg == "--recovery") {
        let network = DeepXNetworkConfig::default();
        let http =
            nautilus_deepx::http::DeepXHttpClient::from_network_config(&network, Some(10), None)?;
        let mut fresh = DeepXWsPublicConnection::connect(
            &network,
            None,
            Duration::from_secs(10),
            NonZeroUsize::new(32).unwrap(),
        )
        .await
        .context("failed to reconnect to public WebSocket")?;
        fresh
            .subscribe(2, vec![DeepXWsPublicChannel::Trades])
            .await
            .context("fresh trade acknowledgement failed")?;
        let frame = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let frame = fresh.next_frame().await?;
                if matches!(
                    frame,
                    DeepXWsPublicFrame::Data {
                        channel: DeepXWsPublicChannel::Trades,
                        ..
                    }
                ) {
                    let (start, end) = trade_stream.recovery_window(&frame)?;
                    if end > start {
                        return anyhow::Ok(frame);
                    }
                }
            }
        })
        .await
        .context("fresh trade page deadline exceeded")??;
        let (start_ms, end_ms) = trade_stream.recovery_window(&frame)?;
        let request = nautilus_deepx::http::DeepXPerpTradesHistoryRequest {
            market_id: 2,
            start_ms,
            end_ms,
            page_size: 100,
            max_pages: 100,
        };
        let history = tokio::time::timeout(
            Duration::from_secs(10),
            http.get_perp_trades_history(&request),
        )
        .await
        .context("REST recovery history deadline exceeded")??;
        let executions = trade_stream.reconcile_history(&history, &frame)?;
        anyhow::ensure!(
            !executions.is_empty(),
            "recovery did not observe an actual new execution"
        );
        println!(
            "DeepX public reconnect REST reconciliation verified start={start_ms} end={end_ms} records={} novel={}",
            history.len(),
            executions.len()
        );
        fresh.close().await?;
    }
    println!("DeepX public connection closed; no private subscriptions or transactions sent");
    Ok(())
}
