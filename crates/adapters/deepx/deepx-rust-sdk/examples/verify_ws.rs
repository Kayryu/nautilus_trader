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

//! Read-only public WebSocket checks using the standalone DeepX SDK protocol types.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    num::NonZeroUsize,
    time::Duration,
};

use anyhow::{Context, ensure};
use deepx_rust_sdk::{
    deployment::{
        DEEPX_TESTNET_GENESIS_HASH, DEEPX_TESTNET_REST_URL, DEEPX_TESTNET_RPC_URL,
        DEEPX_TESTNET_WS_URL,
    },
    enums::DeepXEnvironment,
    identity::decode_account_id20,
    query::{
        DeepXAccountSortOrder, DeepXLendingHistoryInterval, DeepXLendingHistoryRequest,
        DeepXOpenInterestRequest, DeepXPerpAccountTradesRequest, DeepXPerpCandleInterval,
        DeepXPerpCandlesRequest, DeepXPerpTradesHistoryRequest, DeepXSpotAccountTradesRequest,
    },
    rest::{
        DeepXRestTransport,
        models::{DeepXPerpAccountTradeRecord, DeepXSpotAccountTradeRecord},
    },
    rpc::{
        DeepXRpcBlockHeader, DeepXRpcClient, DeepXRpcRuntimeVersion,
        chain_decode::{decode_block_number_u64, decode_prefixed_hash},
        endpoint_identity::{
            DEEPX_RECOVERY_RPC_METHODS, DEEPX_SUBMISSION_RPC_METHODS, DEEPX_WATCH_RPC_METHODS,
            DeepXObservedRpcEndpoint, DeepXRpcRole, validate_rpc_endpoint_identities,
        },
        event_storage::get_system_events,
        events::{DeepXFinalizedPerpTrade, DeepXFinalizedSpotTrade},
        get_finalized_head, get_metadata, get_runtime_version,
        trade_event_decode::{
            decode_finalized_perp_trade_events, decode_finalized_spot_trade_events,
        },
    },
    signing::validate_testnet_runtime,
    urls::ws_connection_url,
    websocket::{
        account::{DeepXWsAccountWireFrame, DeepXWsUserBalancesRequest},
        account_metrics::{
            DeepXWsAccountMetric, DeepXWsAccountMetricChannel, DeepXWsAccountMetricsFrame,
            DeepXWsAccountMetricsRequest,
        },
        account_order_history::{
            DeepXWsOrderHistoryControlFrame, DeepXWsPerpOrderHistoryRequest,
            DeepXWsSpotOrderHistoryRequest,
        },
        account_orders::{DeepXWsPerpOpenOrdersFrame, DeepXWsPerpOpenOrdersRequest},
        account_positions::{
            DeepXWsPerpOpenPositionsFrame, DeepXWsPerpOpenPositionsPage,
            DeepXWsPerpOpenPositionsRequest,
        },
        account_spot_orders::{DeepXWsSpotOpenOrdersFrame, DeepXWsSpotOpenOrdersRequest},
        account_spot_trade_history::{
            DeepXWsSpotFinalizedTradeScope, DeepXWsSpotTradeHistoryBatch,
            DeepXWsSpotTradeHistoryFrame, DeepXWsSpotTradeHistoryRequest,
            match_spot_trade_history_batch_to_finalized, match_spot_trade_history_batch_to_rest,
        },
        account_trade_history::{
            DeepXWsPerpFinalizedTradeScope, DeepXWsPerpTradeHistoryBatch,
            DeepXWsPerpTradeHistoryFrame, DeepXWsPerpTradeHistoryRequest,
            match_perp_trade_history_batch_to_finalized, match_perp_trade_history_batch_to_rest,
        },
        book::DeepXWsBookStream,
        candles::DeepXWsCandleStream,
        lending_rates::{
            DeepXWsLendingRatesFrame, DeepXWsLendingRatesRequest, match_lending_rate_batch_to_rest,
        },
        open_interest::{match_open_interest_to_rest, parse_open_interest},
        public::{DeepXWsPublicChannel, DeepXWsPublicFrame, DeepXWsPublicRequest, validate_ack},
        spot_trades::{DeepXWsSpotTradeFrame, DeepXWsSpotTradeRequest},
        trades::{DeepXWsSpotTradeStream, DeepXWsTradeStream},
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn connect_public() -> anyhow::Result<Socket> {
    let url = ws_connection_url(DEEPX_TESTNET_WS_URL)?;
    let (socket, _) = tokio::time::timeout(Duration::from_secs(10), connect_async(url))
        .await
        .context("WebSocket upgrade deadline exceeded")??;
    Ok(socket)
}

enum Mode {
    Connection,
    Prices,
    Public {
        recovery: bool,
    },
    Book {
        market_id: u16,
        depth: NonZeroUsize,
        updates: bool,
    },
    Balances(String),
    OpenInterest {
        market_id: u16,
        reconcile: bool,
    },
    LendingRates {
        reconcile: bool,
    },
    Candles {
        market_id: u16,
        pair: String,
    },
    PerpOrders {
        wallet: bool,
        address: String,
        market_id: u16,
    },
    PerpPositions {
        wallet: bool,
        address: String,
        market_id: u16,
    },
    SpotOrders {
        wallet: bool,
        address: String,
        market_name: String,
    },
    PerpTradeHistory {
        wallet: String,
        market_id: u16,
        mode: PerpTradeMode,
    },
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum PerpTradeMode {
    Ack,
    Observe,
    Rest,
    Finalized,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SpotTradeMode {
    Observe,
    Rest,
    Finalized,
}

#[derive(Clone, Copy)]
enum FinalizedTradeKind {
    Perp,
    Spot,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().context(
        "usage: deepx-sdk-verify-ws <connection|prices|public|book|spot-live-trades|balances|account-metrics|open-interest|lending-rates|candles|perp-open-orders|perp-open-positions|spot-open-orders|perp-order-history|spot-order-history|perp-trade-history|spot-trade-history> [arguments]",
    )?;
    if mode == "spot-live-trades" {
        let market_name = args.next().context("missing Spot market name")?;
        ensure!(args.next().is_none(), "unexpected argument");
        return verify_spot_live_trades(&market_name).await;
    }
    if mode == "account-metrics" {
        let wallet = args.next().context("missing wallet")?;
        ensure!(args.next().is_none(), "unexpected argument");
        return verify_account_metrics(&wallet).await;
    }
    if mode == "perp-order-history" || mode == "spot-order-history" {
        let wallet = args.next().context("missing wallet")?;
        let market = args.next().context("missing market ID or name")?;
        ensure!(args.next().is_none(), "unexpected argument");
        return verify_order_history(&wallet, &market, mode == "spot-order-history").await;
    }
    if mode == "spot-trade-history" {
        let wallet = args.next().context("missing wallet")?;
        let market_name = args.next().context("missing Spot market name")?;
        let trade_mode = match args.next().as_deref() {
            None => SpotTradeMode::Observe,
            Some("--reconcile") => SpotTradeMode::Rest,
            Some("--finalized") => SpotTradeMode::Finalized,
            Some(_) => anyhow::bail!("expected --reconcile or --finalized"),
        };
        ensure!(args.next().is_none(), "unexpected argument");
        return verify_spot_trade_history(&wallet, &market_name, trade_mode).await;
    }
    let mode = match mode.as_str() {
        "connection" => Mode::Connection,
        "prices" => Mode::Prices,
        "public" => {
            let recovery = match args.next().as_deref() {
                None => false,
                Some("--recovery") => true,
                Some(_) => anyhow::bail!("expected --recovery"),
            };
            Mode::Public { recovery }
        }
        "book" => {
            let market_id = args.next().context("missing market ID")?.parse::<u16>()?;
            ensure!(market_id > 0, "market ID must be positive");
            let depth = args
                .next()
                .context("missing book depth")?
                .parse::<usize>()?;
            let depth = NonZeroUsize::new(depth).context("book depth must be positive")?;
            let updates = match args.next().as_deref() {
                None => false,
                Some("--updates") => true,
                Some(_) => anyhow::bail!("expected --updates"),
            };
            Mode::Book {
                market_id,
                depth,
                updates,
            }
        }
        "balances" => Mode::Balances(args.next().context("missing subaccount")?),
        "open-interest" => {
            let market_id = args.next().context("missing market ID")?.parse::<u16>()?;
            ensure!(market_id > 0, "market ID must be positive");
            let reconcile = match args.next().as_deref() {
                None => false,
                Some("--reconcile") => true,
                Some(_) => anyhow::bail!("expected --reconcile after market ID"),
            };
            Mode::OpenInterest {
                market_id,
                reconcile,
            }
        }
        "lending-rates" => {
            let reconcile = match args.next().as_deref() {
                None => false,
                Some("--reconcile") => true,
                Some(_) => anyhow::bail!("expected --reconcile"),
            };
            Mode::LendingRates { reconcile }
        }
        "candles" => Mode::Candles {
            market_id: args.next().context("missing market ID")?.parse::<u16>()?,
            pair: args.next().context("missing pair")?,
        },
        "perp-open-orders" | "perp-open-positions" => {
            let first = args.next().context("missing address or --wallet")?;
            let wallet = first == "--wallet";
            let address = if wallet {
                args.next().context("missing wallet")?
            } else {
                first
            };
            let market_id = args.next().context("missing market ID")?.parse::<u16>()?;
            if mode == "perp-open-orders" {
                Mode::PerpOrders {
                    wallet,
                    address,
                    market_id,
                }
            } else {
                Mode::PerpPositions {
                    wallet,
                    address,
                    market_id,
                }
            }
        }
        "spot-open-orders" => {
            let first = args.next().context("missing address or --wallet")?;
            let wallet = first == "--wallet";
            let address = if wallet {
                args.next().context("missing wallet")?
            } else {
                first
            };
            let market_name = args.next().context("missing Spot market name")?;
            Mode::SpotOrders {
                wallet,
                address,
                market_name,
            }
        }
        "perp-trade-history" => {
            let wallet = args.next().context("missing wallet")?;
            let market_id = args.next().context("missing market ID")?.parse::<u16>()?;
            let mode = match args.next().as_deref() {
                None => PerpTradeMode::Ack,
                Some("--observe") => PerpTradeMode::Observe,
                Some("--rest") => PerpTradeMode::Rest,
                Some("--reconcile") => PerpTradeMode::Finalized,
                Some(_) => anyhow::bail!("expected --observe, --rest, or --reconcile"),
            };
            Mode::PerpTradeHistory {
                wallet,
                market_id,
                mode,
            }
        }
        _ => anyhow::bail!("unknown WebSocket verification mode: {mode}"),
    };
    ensure!(args.next().is_none(), "unexpected argument");
    let mut socket = connect_public().await?;
    let close_initial_socket = !matches!(mode, Mode::Public { recovery: true });
    match mode {
        Mode::Connection => println!("public WebSocket upgrade verified"),
        Mode::Prices => verify_prices(&mut socket).await?,
        Mode::Public { recovery } => verify_public(&mut socket, recovery).await?,
        Mode::Book {
            market_id,
            depth,
            updates,
        } => verify_book(&mut socket, market_id, depth, updates).await?,
        Mode::Balances(subaccount) => verify_balances(&mut socket, &subaccount).await?,
        Mode::OpenInterest {
            market_id,
            reconcile,
        } => {
            verify_open_interest(&mut socket, market_id, reconcile).await?;
        }
        Mode::LendingRates { reconcile } => verify_lending_rates(&mut socket, reconcile).await?,
        Mode::Candles { market_id, pair } => {
            verify_candles(&mut socket, market_id, &pair).await?;
        }
        Mode::PerpOrders {
            wallet,
            address,
            market_id,
        } => {
            verify_perp_orders(&mut socket, wallet, &address, market_id).await?;
        }
        Mode::PerpPositions {
            wallet,
            address,
            market_id,
        } => {
            verify_perp_positions(&mut socket, wallet, &address, market_id).await?;
        }
        Mode::SpotOrders {
            wallet,
            address,
            market_name,
        } => {
            verify_spot_orders(&mut socket, wallet, &address, &market_name).await?;
        }
        Mode::PerpTradeHistory {
            wallet,
            market_id,
            mode,
        } => {
            verify_perp_trade_history(&mut socket, &wallet, market_id, mode).await?;
        }
    }
    if close_initial_socket {
        socket.close(None).await?;
    }
    Ok(())
}

async fn subscribe(
    socket: &mut Socket,
    market_id: u16,
    channels: &[DeepXWsPublicChannel],
) -> anyhow::Result<VecDeque<DeepXWsPublicFrame>> {
    let request = DeepXWsPublicRequest::Subscribe {
        market_id,
        channels: channels.to_vec(),
    };
    subscribe_request(socket, request, market_id, channels).await
}

async fn subscribe_request(
    socket: &mut Socket,
    request: DeepXWsPublicRequest,
    market_id: u16,
    channels: &[DeepXWsPublicChannel],
) -> anyhow::Result<VecDeque<DeepXWsPublicFrame>> {
    socket
        .send(Message::Text(request.to_text()?.into()))
        .await?;
    let expected = channels.iter().copied().collect::<BTreeSet<_>>();
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut buffered = VecDeque::new();
        loop {
            let frame = next_frame(socket).await?;
            match &frame {
                DeepXWsPublicFrame::Subscribed { .. } => {
                    validate_ack(&frame, market_id, &expected)?;
                    break anyhow::Ok(buffered);
                }
                DeepXWsPublicFrame::Data {
                    market, channel, ..
                } => {
                    ensure!(
                        market.id == market_id && expected.contains(channel),
                        "pre-acknowledgement public data scope mismatch"
                    );
                    ensure!(
                        buffered.len() < 32,
                        "public pre-acknowledgement buffer overflow"
                    );
                    buffered.push_back(frame);
                }
                DeepXWsPublicFrame::Pong { .. } => {}
                DeepXWsPublicFrame::Error { code, message } => {
                    anyhow::bail!("public subscription error {code}: {message}");
                }
                DeepXWsPublicFrame::SubscriptionsChanged { .. } => {
                    anyhow::bail!("unexpected public subscription change before acknowledgement");
                }
            }
        }
    })
    .await
    .context("subscription acknowledgement deadline exceeded")?
}

async fn next_buffered_frame(
    socket: &mut Socket,
    buffered: &mut VecDeque<DeepXWsPublicFrame>,
) -> anyhow::Result<DeepXWsPublicFrame> {
    if let Some(frame) = buffered.pop_front() {
        Ok(frame)
    } else {
        next_frame(socket).await
    }
}

async fn next_frame(socket: &mut Socket) -> anyhow::Result<DeepXWsPublicFrame> {
    DeepXWsPublicFrame::parse(&next_text(socket).await?)
}

async fn next_text(socket: &mut Socket) -> anyhow::Result<String> {
    loop {
        match socket.next().await.context("WebSocket closed")?? {
            Message::Text(text) => return Ok(text.to_string()),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Pong(_) => {}
            Message::Close(_) => anyhow::bail!("WebSocket closed before verification completed"),
            _ => anyhow::bail!("unsupported WebSocket frame during text subscription"),
        }
    }
}

async fn verify_balances(socket: &mut Socket, subaccount: &str) -> anyhow::Result<()> {
    let request = DeepXWsUserBalancesRequest::new(subaccount)?;
    let mut buffered = VecDeque::new();
    socket
        .send(Message::Text(request.to_text()?.into()))
        .await?;
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            match DeepXWsAccountWireFrame::parse(&next_text(socket).await?)? {
                DeepXWsAccountWireFrame::Subscribed {
                    market,
                    subscriptions,
                    ..
                } => {
                    request.validate_ack(&market, &subscriptions)?;
                    break;
                }
                DeepXWsAccountWireFrame::Error { code, message } => {
                    anyhow::bail!("account subscription error {code}: {message}");
                }
                frame @ DeepXWsAccountWireFrame::UserBalances { .. } => {
                    if let DeepXWsAccountWireFrame::UserBalances {
                        ref market,
                        ref balances,
                        ..
                    } = frame
                    {
                        ensure!(
                            market.kind == "all"
                                && balances.address.eq_ignore_ascii_case(request.subaccount()),
                            "pre-ack account balance frame belongs to another subaccount"
                        );
                    }
                    ensure!(buffered.len() < 8, "account pre-ack buffer overflow");
                    buffered.push_back(frame);
                }
                DeepXWsAccountWireFrame::Pong { .. } => {}
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("account subscription acknowledgement deadline exceeded")??;
    tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            let frame = if let Some(frame) = buffered.pop_front() {
                frame
            } else {
                DeepXWsAccountWireFrame::parse(&next_text(socket).await?)?
            };
            match frame {
                DeepXWsAccountWireFrame::UserBalances {
                    market,
                    balances,
                    timestamp,
                } => {
                    ensure!(
                        market.kind == "all"
                            && balances.address.eq_ignore_ascii_case(request.subaccount()),
                        "account balance frame belongs to another subaccount"
                    );
                    println!(
                        "account balances verified: address={} assets={} timestamp_ms={timestamp}",
                        balances.address,
                        balances.assets.len()
                    );
                    break;
                }
                DeepXWsAccountWireFrame::Error { code, message } => {
                    anyhow::bail!("account data error {code}: {message}");
                }
                DeepXWsAccountWireFrame::Pong { .. } => {}
                DeepXWsAccountWireFrame::Subscribed { .. } => {
                    anyhow::bail!("duplicate account balance acknowledgement");
                }
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("account balance snapshot deadline exceeded")??;
    Ok(())
}

async fn verify_account_metrics(wallet: &str) -> anyhow::Result<()> {
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let directory = transport
        .get_wallet_subaccounts(DEEPX_TESTNET_REST_URL, wallet)
        .await?;
    ensure!(
        !directory.addresses.is_empty(),
        "wallet subaccount directory is empty"
    );
    for subaccount in &directory.addresses {
        let request = DeepXWsAccountMetricsRequest::new(subaccount)?;
        let mut socket = connect_public().await?;
        let metrics = receive_account_metrics(&mut socket, &request).await?;
        for metric in metrics.values() {
            println!(
                "account metric: subaccount={} channel={:?} raw_data={} timestamp_ms={}",
                request.subaccount(),
                metric.channel(),
                metric.raw_data().get(),
                metric.timestamp_ms()
            );
        }
        socket.close(None).await?;
    }
    println!(
        "account summaries verified: wallet={} subaccounts={}",
        directory.wallet(),
        directory.addresses.len()
    );
    Ok(())
}

async fn receive_account_metrics(
    socket: &mut Socket,
    request: &DeepXWsAccountMetricsRequest,
) -> anyhow::Result<BTreeMap<DeepXWsAccountMetricChannel, DeepXWsAccountMetric>> {
    socket.send(Message::Text(request.to_text().into())).await?;
    tokio::time::timeout(Duration::from_secs(35), async {
        let mut acknowledged = false;
        let mut pre_ack_frames = 0;
        let mut metrics: BTreeMap<DeepXWsAccountMetricChannel, DeepXWsAccountMetric> =
            BTreeMap::new();
        while !acknowledged || metrics.len() < DeepXWsAccountMetricChannel::ALL.len() {
            match DeepXWsAccountMetricsFrame::parse(&next_text(socket).await?, request)? {
                DeepXWsAccountMetricsFrame::Subscribed => {
                    ensure!(!acknowledged, "duplicate account metric acknowledgement");
                    acknowledged = true;
                }
                DeepXWsAccountMetricsFrame::Metric(metric) => {
                    if !acknowledged {
                        pre_ack_frames += 1;
                        ensure!(
                            pre_ack_frames <= DeepXWsAccountMetricChannel::ALL.len(),
                            "account metric pre-ack buffer overflow"
                        );
                    }
                    if let Some(previous) = metrics.get(&metric.channel()) {
                        ensure!(
                            previous.raw_data().get() == metric.raw_data().get(),
                            "conflicting duplicate account metric data"
                        );
                    } else {
                        metrics.insert(metric.channel(), *metric);
                    }
                }
                DeepXWsAccountMetricsFrame::Pong => {}
            }
        }
        anyhow::Ok(metrics)
    })
    .await
    .context("account metric deadline exceeded")?
}

async fn verify_open_interest(
    socket: &mut Socket,
    market_id: u16,
    reconcile: bool,
) -> anyhow::Result<()> {
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let markets = transport.get_perp_markets(DEEPX_TESTNET_REST_URL).await?;
    let market = markets
        .iter()
        .find(|market| market.id == u64::from(market_id))
        .context("market ID is absent from the REST directory")?;
    let mut buffered = subscribe(socket, market_id, &[DeepXWsPublicChannel::OpenInterest]).await?;
    let observation = tokio::time::timeout(Duration::from_secs(75), async {
        loop {
            if let Some(observation) = parse_open_interest(
                next_buffered_frame(socket, &mut buffered).await?,
                market_id,
                &market.name,
            )? {
                break anyhow::Ok(observation);
            }
        }
    })
    .await
    .context("open-interest bucket deadline exceeded")??;
    if reconcile {
        let request = DeepXOpenInterestRequest {
            market_id: u64::from(market_id),
            start_ms: observation.record.statistic_time,
            end_ms: Some(observation.record.statistic_time),
            limit: Some(1),
        };
        let rest = transport
            .get_perp_open_interest_history(DEEPX_TESTNET_REST_URL, &request, 1)
            .await?;
        match_open_interest_to_rest(&observation, &rest)?;
    }
    println!(
        "open interest observed: market={} pair={} statistic_time_ms={} total_oi={} rest_reconciled={reconcile}",
        observation.market_id,
        observation.pair,
        observation.record.statistic_time,
        observation.record.total_oi
    );
    Ok(())
}

async fn verify_lending_rates(socket: &mut Socket, reconcile: bool) -> anyhow::Result<()> {
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let directory = transport.get_lending_assets(DEEPX_TESTNET_REST_URL).await?;
    let request = DeepXWsLendingRatesRequest::new(
        directory
            .iter()
            .map(|asset| (asset.market_id, asset.asset.clone())),
    )?;
    socket.send(Message::Text(request.to_text().into())).await?;
    let batch = tokio::time::timeout(Duration::from_secs(35), async {
        let mut acknowledged = false;
        loop {
            match request.parse(&next_text(socket).await?)? {
                DeepXWsLendingRatesFrame::Subscribed => {
                    ensure!(!acknowledged, "duplicate lending-rate acknowledgement");
                    acknowledged = true;
                }
                DeepXWsLendingRatesFrame::Rates(batch) if acknowledged => break anyhow::Ok(batch),
                DeepXWsLendingRatesFrame::Rates(_) => {
                    anyhow::bail!("lending rate data arrived before acknowledgement");
                }
                DeepXWsLendingRatesFrame::Pong => {}
            }
        }
    })
    .await
    .context("lending-rate batch deadline exceeded")??;
    if reconcile {
        let end_ms = batch
            .statistic_time_ms
            .checked_add(60_000)
            .context("lending statistic time exceeds one-minute range")?;
        let mut records = Vec::with_capacity(batch.rates.len());
        for rate in &batch.rates {
            let history = transport
                .get_lending_interest_rate_history(
                    DEEPX_TESTNET_REST_URL,
                    &DeepXLendingHistoryRequest {
                        market_id: Some(rate.market_id),
                        asset: Some(rate.asset.clone()),
                        interval: DeepXLendingHistoryInterval::OneMinute,
                        start_ms: batch.statistic_time_ms,
                        end_ms: Some(end_ms),
                        limit: Some(2),
                        sort: DeepXAccountSortOrder::Ascending,
                    },
                )
                .await?;
            records.extend(history.details);
        }
        match_lending_rate_batch_to_rest(&batch, &records)?;
    }
    for rate in &batch.rates {
        println!(
            "market_id={} asset={} borrow_apr={} supply_apr={} statistic_time_ms={} rest_reconciled={reconcile}",
            rate.market_id, rate.asset, rate.borrow_apr, rate.supply_apr, batch.statistic_time_ms
        );
    }
    Ok(())
}

async fn verify_candles(socket: &mut Socket, market_id: u16, pair: &str) -> anyhow::Result<()> {
    let mut stream = DeepXWsCandleStream::new(market_id, pair)?;
    socket
        .send(Message::Text(stream.subscription_text()?.into()))
        .await?;
    let superseded = tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            if let Some(candle) = stream.apply(&next_text(socket).await?)? {
                break anyhow::Ok(candle);
            }
        }
    })
    .await
    .context("superseded candle deadline exceeded")??;
    let candle = superseded.candle;
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let request = DeepXPerpCandlesRequest {
        market_id: u64::from(market_id),
        interval: DeepXPerpCandleInterval::OneMinute,
        start_ms: candle.time,
        end_ms: Some(
            candle
                .time
                .checked_add(60_000)
                .context("candle time overflow")?,
        ),
        limit: Some(1),
    };
    let checks = tokio::time::timeout(Duration::from_secs(90), async {
        let mut checks = 0;
        loop {
            let page = transport
                .get_perp_candles(DEEPX_TESTNET_REST_URL, &request)
                .await?;
            checks += 1;
            ensure!(page.pair == pair, "REST candle pair identity mismatch");
            ensure!(
                page.details.len() <= 1,
                "REST returned multiple candle buckets"
            );
            if page.details.first() == Some(&candle) {
                break anyhow::Ok(checks);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .context("REST candle did not converge with WebSocket within 90 seconds")??;
    println!(
        "superseded 1m candle verified: market={market_id} time_ms={} close={} volume={} gap_after={} rest_checks={checks}",
        candle.time, candle.close, candle.volume, superseded.gap_after
    );
    Ok(())
}

async fn wallet_subaccounts(wallet: &str) -> anyhow::Result<Vec<String>> {
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let directory = transport
        .get_wallet_subaccounts(DEEPX_TESTNET_REST_URL, wallet)
        .await?;
    ensure!(
        !directory.addresses.is_empty(),
        "wallet subaccount directory is empty"
    );
    Ok(directory.addresses)
}

async fn verify_perp_orders(
    socket: &mut Socket,
    wallet: bool,
    address: &str,
    market_id: u16,
) -> anyhow::Result<()> {
    let request = if wallet {
        DeepXWsPerpOpenOrdersRequest::for_wallet(
            market_id,
            address,
            &wallet_subaccounts(address).await?,
        )?
    } else {
        DeepXWsPerpOpenOrdersRequest::new(market_id, address)?
    };
    socket
        .send(Message::Text(request.to_text()?.into()))
        .await?;
    tokio::time::timeout(Duration::from_secs(35), async {
        let mut acknowledged = false;
        let mut observed = HashSet::new();
        while observed.len() < request.expected_page_count() {
            match DeepXWsPerpOpenOrdersFrame::parse(&next_text(socket).await?, &request)? {
                DeepXWsPerpOpenOrdersFrame::Subscribed => {
                    ensure!(!acknowledged, "duplicate perpetual order acknowledgement");
                    acknowledged = true;
                }
                DeepXWsPerpOpenOrdersFrame::OrderPage { page, timestamp_ms } => {
                    ensure!(acknowledged, "order page arrived before acknowledgement");
                    ensure!(observed.insert(page.subaccount().to_ascii_lowercase()),
                        "duplicate initial order page");
                    println!(
                        "perp open orders: subaccount={} market_id={market_id} raw_orders={} timestamp_ms={timestamp_ms}",
                        page.subaccount(), page.items().len()
                    );
                }
                DeepXWsPerpOpenOrdersFrame::Pong => {}
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("perpetual open-order snapshot deadline exceeded")??;
    Ok(())
}

async fn verify_perp_positions(
    socket: &mut Socket,
    wallet: bool,
    address: &str,
    market_id: u16,
) -> anyhow::Result<()> {
    let request = if wallet {
        DeepXWsPerpOpenPositionsRequest::for_wallet(
            market_id,
            address,
            &wallet_subaccounts(address).await?,
        )?
    } else {
        DeepXWsPerpOpenPositionsRequest::new(market_id, address)?
    };
    socket
        .send(Message::Text(request.to_text()?.into()))
        .await?;
    tokio::time::timeout(Duration::from_secs(35), async {
        let mut acknowledged = false;
        let mut observed: BTreeMap<String, DeepXWsPerpOpenPositionsPage> = BTreeMap::new();
        while observed.len() < request.expected_page_count() {
            match DeepXWsPerpOpenPositionsFrame::parse(&next_text(socket).await?, &request)? {
                DeepXWsPerpOpenPositionsFrame::Subscribed => {
                    ensure!(!acknowledged, "duplicate perpetual position acknowledgement");
                    acknowledged = true;
                }
                DeepXWsPerpOpenPositionsFrame::PositionPage { page, timestamp_ms } => {
                    ensure!(acknowledged, "position page arrived before acknowledgement");
                    let address = page.subaccount().to_ascii_lowercase();
                    if let Some(previous) = observed.get(&address) {
                        ensure!(
                            previous.same_contents(&page),
                            "conflicting duplicate initial position page"
                        );
                        continue;
                    }
                    println!(
                        "perp open positions: subaccount={} market_id={market_id} raw_positions={} timestamp_ms={timestamp_ms}",
                        page.subaccount(), page.items().len()
                    );
                    observed.insert(address, page);
                }
                DeepXWsPerpOpenPositionsFrame::Pong => {}
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("perpetual open-position snapshot deadline exceeded")??;
    Ok(())
}

async fn verify_spot_orders(
    socket: &mut Socket,
    wallet: bool,
    address: &str,
    market_name: &str,
) -> anyhow::Result<()> {
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let market = transport
        .get_spot_markets(DEEPX_TESTNET_REST_URL)
        .await?
        .into_iter()
        .find(|market| market.name == market_name)
        .context("Spot market is absent from the current directory")?;
    let request = if wallet {
        DeepXWsSpotOpenOrdersRequest::for_wallet(
            &market,
            address,
            &wallet_subaccounts(address).await?,
        )?
    } else {
        DeepXWsSpotOpenOrdersRequest::new(&market, address)?
    };
    socket.send(Message::Text(request.to_text().into())).await?;
    tokio::time::timeout(Duration::from_secs(35), async {
        let mut acknowledged = false;
        let mut observed = HashSet::new();
        while observed.len() < request.expected_page_count() {
            match DeepXWsSpotOpenOrdersFrame::parse(&next_text(socket).await?, &request)? {
                DeepXWsSpotOpenOrdersFrame::Subscribed => {
                    ensure!(!acknowledged, "duplicate Spot order acknowledgement");
                    acknowledged = true;
                }
                DeepXWsSpotOpenOrdersFrame::OrderPage { page, timestamp_ms } => {
                    ensure!(acknowledged, "Spot order page arrived before acknowledgement");
                    ensure!(observed.insert(page.subaccount().to_ascii_lowercase()),
                        "duplicate initial Spot order page");
                    println!(
                        "Spot open orders: subaccount={} market={} raw_orders={} timestamp_ms={timestamp_ms}",
                        page.subaccount(), request.market_name(), page.items().len()
                    );
                }
                DeepXWsSpotOpenOrdersFrame::Pong => {}
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("Spot open-order snapshot deadline exceeded")??;
    Ok(())
}

async fn verify_order_history(
    wallet: &str,
    selected_market: &str,
    spot: bool,
) -> anyhow::Result<()> {
    let subaccounts = wallet_subaccounts(wallet).await?;
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let spot_market = if spot {
        Some(
            transport
                .get_spot_markets(DEEPX_TESTNET_REST_URL)
                .await?
                .into_iter()
                .find(|market| market.name == selected_market)
                .context("Spot market is absent from the current directory")?,
        )
    } else {
        None
    };
    let perp_market_id = if spot {
        None
    } else {
        let market_id = selected_market.parse::<u16>()?;
        ensure!(
            transport
                .get_perp_markets(DEEPX_TESTNET_REST_URL)
                .await?
                .iter()
                .any(|market| market.id == u64::from(market_id)),
            "perpetual market is absent from the current directory"
        );
        Some(market_id)
    };
    for subaccount in &subaccounts {
        let mut socket = connect_public().await?;
        if let Some(market) = &spot_market {
            let request = DeepXWsSpotOrderHistoryRequest::new(market, subaccount)?;
            socket.send(Message::Text(request.to_text().into())).await?;
            receive_order_history_ack(&mut socket, |text| request.parse_control_frame(text))
                .await?;
        } else {
            let market_id = perp_market_id.context("missing perpetual market ID")?;
            let request = DeepXWsPerpOrderHistoryRequest::new(market_id, subaccount)?;
            socket.send(Message::Text(request.to_text().into())).await?;
            receive_order_history_ack(&mut socket, |text| request.parse_control_frame(text))
                .await?;
        }
        socket.close(None).await?;
    }
    println!(
        "order-history subscriptions acknowledged: wallet={wallet} market={selected_market} spot={spot} subaccounts={}",
        subaccounts.len()
    );
    Ok(())
}

async fn receive_order_history_ack(
    socket: &mut Socket,
    parse: impl Fn(&str) -> anyhow::Result<DeepXWsOrderHistoryControlFrame>,
) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match parse(&next_text(socket).await?)? {
                DeepXWsOrderHistoryControlFrame::Subscribed => break anyhow::Ok(()),
                DeepXWsOrderHistoryControlFrame::Pong => {}
            }
        }
    })
    .await
    .context("order-history acknowledgement deadline exceeded")??;
    Ok(())
}

async fn verify_perp_trade_history(
    socket: &mut Socket,
    wallet: &str,
    market_id: u16,
    mode: PerpTradeMode,
) -> anyhow::Result<()> {
    let subaccounts = wallet_subaccounts(wallet).await?;
    let request = DeepXWsPerpTradeHistoryRequest::for_wallet(market_id, wallet, &subaccounts)?;
    socket
        .send(Message::Text(request.to_text()?.into()))
        .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut acknowledged = false;
    let mut batches = Vec::new();
    let mut items = 0_usize;
    loop {
        let text = match tokio::time::timeout_at(deadline, next_text(socket)).await {
            Ok(result) => result?,
            Err(_) => {
                ensure!(
                    acknowledged,
                    "perpetual trade-history acknowledgement timed out"
                );
                break;
            }
        };
        match DeepXWsPerpTradeHistoryFrame::parse(&text, &request)? {
            DeepXWsPerpTradeHistoryFrame::Subscribed => {
                ensure!(
                    !acknowledged,
                    "duplicate perpetual trade-history acknowledgement"
                );
                acknowledged = true;
                if mode == PerpTradeMode::Ack {
                    break;
                }
            }
            DeepXWsPerpTradeHistoryFrame::TradeBatch(batch) => {
                ensure!(acknowledged, "trade data preceded acknowledgement");
                items = items
                    .checked_add(batch.items().len())
                    .context("trade count overflowed")?;
                ensure!(
                    items
                        <= if matches!(mode, PerpTradeMode::Rest | PerpTradeMode::Finalized) {
                            100
                        } else {
                            10_000
                        },
                    "perpetual trade-history item limit exceeded"
                );
                if mode == PerpTradeMode::Observe {
                    for item in batch.items() {
                        println!(
                            "subaccount={} timestamp_ms={} trade={}",
                            batch.subaccount(),
                            batch.timestamp_ms(),
                            item.raw().get()
                        );
                    }
                }
                batches.push(batch);
                if batches.len()
                    == if matches!(mode, PerpTradeMode::Rest | PerpTradeMode::Finalized) {
                        20
                    } else {
                        100
                    }
                {
                    break;
                }
            }
            DeepXWsPerpTradeHistoryFrame::Pong => {}
        }
    }
    if matches!(mode, PerpTradeMode::Rest | PerpTradeMode::Finalized) {
        reconcile_perp_rest(market_id, &batches, mode == PerpTradeMode::Finalized).await?;
    }
    println!(
        "perpetual trade-history {}: wallet={wallet} market_id={market_id} subaccounts={} batches={} items={items}",
        match mode {
            PerpTradeMode::Ack => "acknowledged",
            PerpTradeMode::Observe => "observed",
            PerpTradeMode::Rest => "REST matched",
            PerpTradeMode::Finalized => "finalized matched",
        },
        subaccounts.len(),
        batches.len()
    );
    Ok(())
}

async fn reconcile_perp_rest(
    market_id: u16,
    batches: &[DeepXWsPerpTradeHistoryBatch],
    finalized: bool,
) -> anyhow::Result<()> {
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    if finalized && batches.iter().all(|batch| batch.items().is_empty()) {
        return Ok(());
    }
    let mut seen_ids = HashSet::new();
    let mut block_numbers = BTreeSet::new();
    let mut candidates = Vec::new();
    for batch in batches {
        if batch.items().is_empty() {
            continue;
        }
        let timestamps = batch
            .items()
            .iter()
            .map(|item| {
                let timestamp: jiff::Timestamp = item.created_at.parse()?;
                u64::try_from(timestamp.as_millisecond()).map_err(Into::into)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let request = DeepXPerpAccountTradesRequest {
            subaccount: batch.subaccount().to_string(),
            order_id: None,
            market_id: Some(u64::from(market_id)),
            is_long: None,
            cursor: None,
            sort: DeepXAccountSortOrder::Descending,
            start_ms: timestamps.iter().min().copied(),
            end_ms: timestamps.iter().max().copied(),
            page_size: Some(100),
        };
        let records = transport
            .get_perp_account_trade_pages(DEEPX_TESTNET_REST_URL, &request, 10)
            .await?
            .into_iter()
            .flat_map(|page| page.items)
            .collect::<Vec<DeepXPerpAccountTradeRecord>>();
        for record in match_perp_trade_history_batch_to_rest(batch, &request, &records)? {
            ensure!(
                seen_ids.insert(record.id),
                "duplicate perpetual trade across observed WebSocket batches"
            );
            if finalized {
                let block_number = record.id / 1_000_000;
                ensure!(block_number > 0, "invalid perpetual trade block identity");
                block_numbers.insert(block_number);
            } else {
                println!(
                    "subaccount={} rest_trade_id={} order_id={}",
                    batch.subaccount(),
                    record.id,
                    record.order_id
                );
            }
        }
        if finalized {
            candidates.push((batch, request, records));
        }
    }
    if finalized {
        ensure!(
            block_numbers.len() <= 20,
            "perpetual trade finalized block budget exceeded"
        );
        let (events, _) = observe_finalized_trades(block_numbers, FinalizedTradeKind::Perp).await?;
        let market = transport
            .get_perp_market_by_id(DEEPX_TESTNET_REST_URL, u64::from(market_id))
            .await?;
        let mut lines = Vec::new();
        for (batch, request, records) in candidates {
            let scope = DeepXWsPerpFinalizedTradeScope {
                subaccount: decode_account_id20(batch.subaccount())
                    .context("invalid perpetual trade subaccount")?,
                base_decimal: market.base_decimal,
                quote_decimal: market.quote_decimal,
                quote_symbol: &market.quote_symbol,
            };
            for (record, trade) in match_perp_trade_history_batch_to_finalized(
                batch, &request, &records, &events, scope,
            )? {
                lines.push(format!(
                    "subaccount={} rest_trade_id={} block={} event={}",
                    batch.subaccount(),
                    record.id,
                    trade.block_number,
                    trade.event_index
                ));
            }
        }
        for line in lines {
            println!("{line}");
        }
    }
    Ok(())
}

async fn verify_spot_trade_history(
    wallet: &str,
    market_name: &str,
    mode: SpotTradeMode,
) -> anyhow::Result<()> {
    let subaccounts = wallet_subaccounts(wallet).await?;
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let market = transport
        .get_spot_markets(DEEPX_TESTNET_REST_URL)
        .await?
        .into_iter()
        .find(|market| market.name == market_name)
        .context("Spot market is absent from the current directory")?;
    let mut total_batches = 0;
    let mut total_items = 0;
    let mut observations: Vec<(
        DeepXWsSpotTradeHistoryRequest,
        Vec<DeepXWsSpotTradeHistoryBatch>,
    )> = Vec::with_capacity(subaccounts.len());
    for subaccount in &subaccounts {
        let request = DeepXWsSpotTradeHistoryRequest::new(&market, subaccount)?;
        let mut socket = connect_public().await?;
        socket.send(Message::Text(request.to_text().into())).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut acknowledged = false;
        let mut batches = Vec::new();
        let mut items = 0_usize;
        loop {
            let text = match tokio::time::timeout_at(deadline, next_text(&mut socket)).await {
                Ok(result) => result?,
                Err(_) => {
                    ensure!(acknowledged, "Spot trade-history acknowledgement timed out");
                    break;
                }
            };
            match DeepXWsSpotTradeHistoryFrame::parse(&text, &request)? {
                DeepXWsSpotTradeHistoryFrame::Subscribed => {
                    ensure!(
                        !acknowledged,
                        "duplicate Spot trade-history acknowledgement"
                    );
                    acknowledged = true;
                }
                DeepXWsSpotTradeHistoryFrame::TradeBatch(batch) => {
                    ensure!(acknowledged, "Spot trade data preceded acknowledgement");
                    items = items
                        .checked_add(batch.items().len())
                        .context("trade count overflowed")?;
                    ensure!(items <= 100, "Spot trade-history item limit exceeded");
                    if mode == SpotTradeMode::Observe {
                        for item in batch.items() {
                            println!(
                                "subaccount={subaccount} timestamp_ms={} trade={}",
                                batch.timestamp_ms(),
                                item.raw().get()
                            );
                        }
                    }
                    batches.push(batch);
                    if batches.len() == 20 {
                        break;
                    }
                }
                DeepXWsSpotTradeHistoryFrame::Pong => {}
            }
        }
        total_batches += batches.len();
        total_items += items;
        observations.push((request, batches));
        socket.close(None).await?;
    }
    if mode != SpotTradeMode::Observe {
        ensure!(
            total_batches <= 20 && total_items <= 100,
            "Spot trade reconciliation budget exceeded"
        );
        let mut seen_ids = HashSet::with_capacity(total_items);
        let mut block_numbers = BTreeSet::new();
        let mut candidates = Vec::new();
        for (subscription, batches) in &observations {
            for batch in batches {
                if batch.items().is_empty() {
                    continue;
                }
                let timestamps = batch
                    .items()
                    .iter()
                    .map(|item| {
                        let timestamp: jiff::Timestamp = item.record().created_at.parse()?;
                        u64::try_from(timestamp.as_millisecond()).map_err(Into::into)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let request = DeepXSpotAccountTradesRequest {
                    subaccount: subscription.subaccount().to_string(),
                    order_id: None,
                    order_side: None,
                    name: None,
                    pair: Some(market.pair.clone()),
                    cursor: None,
                    sort: DeepXAccountSortOrder::Descending,
                    start_ms: timestamps.iter().min().copied(),
                    end_ms: timestamps.iter().max().copied(),
                    page_size: Some(100),
                };
                let records = transport
                    .get_spot_account_trade_pages(DEEPX_TESTNET_REST_URL, &request, 10)
                    .await?
                    .into_iter()
                    .flat_map(|page| page.items)
                    .collect::<Vec<_>>();
                let matches = match_spot_trade_history_batch_to_rest(
                    batch,
                    subscription,
                    &request,
                    &records,
                )?;
                for record in matches {
                    ensure!(
                        seen_ids.insert(record.id),
                        "duplicate Spot trade across observed WebSocket batches"
                    );
                    if mode == SpotTradeMode::Finalized {
                        let block_number = record.id / 1_000_000;
                        ensure!(block_number > 0, "invalid Spot trade block identity");
                        block_numbers.insert(block_number);
                    } else {
                        println!(
                            "subaccount={} rest_trade_id={} order_id={}",
                            subscription.subaccount(),
                            record.id,
                            record.order_id
                        );
                    }
                }
                if mode == SpotTradeMode::Finalized {
                    candidates.push(SpotTradeCandidate {
                        subscription,
                        batch,
                        request,
                        records,
                    });
                }
            }
        }
        if mode == SpotTradeMode::Finalized {
            ensure!(
                block_numbers.len() <= 20,
                "Spot trade finalized block budget exceeded"
            );
            let (_, events) =
                observe_finalized_trades(block_numbers, FinalizedTradeKind::Spot).await?;
            let mut lines = Vec::new();
            for candidate in candidates {
                let scope = DeepXWsSpotFinalizedTradeScope {
                    subaccount: decode_account_id20(candidate.subscription.subaccount())
                        .context("invalid Spot trade subaccount")?,
                    base_decimal: market.base_decimal,
                    quote_decimal: market.quote_decimal,
                    base_symbol: &market.base_symbol,
                    quote_symbol: &market.quote_symbol,
                };
                for (record, trade) in match_spot_trade_history_batch_to_finalized(
                    candidate.batch,
                    candidate.subscription,
                    &candidate.request,
                    &candidate.records,
                    &events,
                    scope,
                )? {
                    lines.push(format!(
                        "subaccount={} rest_trade_id={} block={} event={}",
                        candidate.subscription.subaccount(),
                        record.id,
                        trade.block_number,
                        trade.event_index
                    ));
                }
            }
            for line in lines {
                println!("{line}");
            }
        }
    }
    println!(
        "Spot trade-history {}: wallet={wallet} market={market_name} subaccounts={} batches={total_batches} items={total_items}",
        match mode {
            SpotTradeMode::Observe => "observed",
            SpotTradeMode::Rest => "REST reconciled",
            SpotTradeMode::Finalized => "finalized matched",
        },
        subaccounts.len()
    );
    Ok(())
}

struct SpotTradeCandidate<'a> {
    subscription: &'a DeepXWsSpotTradeHistoryRequest,
    batch: &'a DeepXWsSpotTradeHistoryBatch,
    request: DeepXSpotAccountTradesRequest,
    records: Vec<DeepXSpotAccountTradeRecord>,
}

async fn observe_finalized_trades(
    block_numbers: BTreeSet<u64>,
    kind: FinalizedTradeKind,
) -> anyhow::Result<(Vec<DeepXFinalizedPerpTrade>, Vec<DeepXFinalizedSpotTrade>)> {
    if block_numbers.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    verify_finalized_rpc_roles().await?;
    let client = DeepXRpcClient::new_with_proxy(
        DEEPX_TESTNET_RPC_URL.to_string(),
        std::env::var("DEEPX_PROXY_URL").ok().as_deref(),
    )?;
    let genesis = client.get_block_hash_required_once(0).await?;
    ensure!(
        genesis.eq_ignore_ascii_case(DEEPX_TESTNET_GENESIS_HASH),
        "RPC endpoint genesis hash differs from approved DeepX testnet"
    );
    let finalized_hash = get_finalized_head(&client).await?;
    let finalized_header: DeepXRpcBlockHeader = client.get_header_once(&finalized_hash).await?;
    let finalized_number = decode_block_number_u64(&finalized_header.number)
        .context("invalid finalized head number")?;
    let mut perp_events = Vec::new();
    let mut spot_events = Vec::new();
    for block_number in block_numbers {
        ensure!(
            block_number <= finalized_number,
            "trade block is above the finalized head"
        );
        let block_hash = client.get_block_hash_required_once(block_number).await?;
        let header: DeepXRpcBlockHeader = client.get_header_once(&block_hash).await?;
        ensure!(
            decode_block_number_u64(&header.number) == Some(block_number),
            "trade canonical header number mismatch"
        );
        let first = get_system_events(&client, &block_hash, block_number).await?;
        let second = get_system_events(&client, &block_hash, block_number).await?;
        ensure!(first == second, "trade event storage changed");
        let version: DeepXRpcRuntimeVersion = get_runtime_version(&client, &block_hash).await?;
        let encoded = get_metadata(&client, &block_hash).await?;
        let metadata = hex::decode(encoded.strip_prefix("0x").context("invalid metadata hex")?)?;
        let genesis_hash = decode_prefixed_hash(DEEPX_TESTNET_GENESIS_HASH)
            .context("invalid approved testnet genesis hash")?;
        let runtime = validate_testnet_runtime(
            &DeepXEnvironment::Testnet,
            genesis_hash,
            version.spec_version,
            version.transaction_version,
            &metadata,
        )?;
        let block = decode_prefixed_hash(&block_hash).context("invalid trade block hash")?;
        let (perp, spot) = match kind {
            FinalizedTradeKind::Perp => (
                decode_finalized_perp_trade_events(
                    runtime.interfaces(),
                    runtime.metadata(),
                    block,
                    block_number,
                    &first,
                )?,
                Vec::new(),
            ),
            FinalizedTradeKind::Spot => (
                Vec::new(),
                decode_finalized_spot_trade_events(
                    runtime.interfaces(),
                    runtime.metadata(),
                    block,
                    block_number,
                    &first,
                )?,
            ),
        };
        let canonical = client.get_block_hash_required_once(block_number).await?;
        ensure!(
            canonical.eq_ignore_ascii_case(&block_hash),
            "trade canonical block hash changed during the read"
        );
        perp_events.extend(perp);
        spot_events.extend(spot);
    }
    Ok((perp_events, spot_events))
}

async fn verify_finalized_rpc_roles() -> anyhow::Result<()> {
    let url = DEEPX_TESTNET_RPC_URL.to_string();
    let proxy = std::env::var("DEEPX_PROXY_URL").ok();
    let clients = [
        DeepXRpcClient::new_with_proxy(url.clone(), proxy.as_deref())?,
        DeepXRpcClient::new_with_proxy(url.clone(), proxy.as_deref())?,
        DeepXRpcClient::new_with_proxy(url.clone(), proxy.as_deref())?,
    ];
    let (submission, watch, recovery) = tokio::try_join!(
        clients[0].get_block_hash_required_once(0),
        clients[1].get_block_hash_required_once(0),
        clients[2].get_block_hash_required_once(0),
    )?;
    let roles = [
        (DeepXRpcRole::Submission, submission),
        (DeepXRpcRole::Watch, watch),
        (DeepXRpcRole::Recovery, recovery),
    ];
    let observed = roles
        .into_iter()
        .map(|(role, encoded)| {
            let genesis = decode_prefixed_hash(&encoded).context("invalid role genesis hash")?;
            anyhow::Ok(DeepXObservedRpcEndpoint::new(role, url.clone(), genesis))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    validate_rpc_endpoint_identities(&url, &url, &url, observed)?;
    let required = [
        (DeepXRpcRole::Submission, DEEPX_SUBMISSION_RPC_METHODS),
        (DeepXRpcRole::Watch, DEEPX_WATCH_RPC_METHODS),
        (DeepXRpcRole::Recovery, DEEPX_RECOVERY_RPC_METHODS),
    ];
    for (client, (role, methods)) in clients.iter().zip(required) {
        let result: Value = client
            .execute_rpc_call(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "rpc_methods",
                "params": [],
            }))
            .await
            .with_context(|| format!("RPC method probe failed for {role:?}"))?;
        let advertised = result["methods"]
            .as_array()
            .with_context(|| format!("RPC methods missing for {role:?}"))?;
        for method in methods {
            ensure!(
                advertised
                    .iter()
                    .any(|value| value.as_str() == Some(method)),
                "RPC endpoint for {role:?} does not advertise {method}"
            );
        }
    }
    Ok(())
}

async fn verify_prices(socket: &mut Socket) -> anyhow::Result<()> {
    let channels = [
        DeepXWsPublicChannel::LatestPrice,
        DeepXWsPublicChannel::OraclePrice,
        DeepXWsPublicChannel::MarkPrice,
        DeepXWsPublicChannel::FundingRate,
    ];
    let mut buffered = subscribe(socket, 3, &channels).await?;
    let mut observed = BTreeSet::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        while observed.len() < channels.len() {
            match next_buffered_frame(socket, &mut buffered).await? {
                DeepXWsPublicFrame::Data {
                    market,
                    channel,
                    data,
                    timestamp,
                } if market.id == 3 && channels.contains(&channel) => {
                    if observed.insert(channel) {
                        println!(
                            "market={} channel={channel:?} timestamp={timestamp} data={data}",
                            market.id
                        );
                    }
                }
                DeepXWsPublicFrame::Pong { .. } => {}
                _ => anyhow::bail!("unexpected or unconfirmed price-channel frame"),
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("price-channel data deadline exceeded")??;
    Ok(())
}

async fn verify_public(socket: &mut Socket, recovery: bool) -> anyhow::Result<()> {
    let channels = [
        DeepXWsPublicChannel::Trades,
        DeepXWsPublicChannel::Orderbook,
    ];
    let mut buffered = subscribe(socket, 2, &channels).await?;
    let mut trade_stream = DeepXWsTradeStream::new(2, NonZeroUsize::new(1024).unwrap());
    let mut book_stream = DeepXWsBookStream::new(2, NonZeroUsize::new(1024).unwrap());
    let mut initial_trade_page = false;
    let mut trade_count = 0;
    let mut book_updates = 0;
    tokio::time::timeout(Duration::from_secs(15), async {
        while trade_count == 0 || book_updates < 2 {
            let frame = next_buffered_frame(socket, &mut buffered).await?;
            match &frame {
                DeepXWsPublicFrame::Data {
                    market,
                    channel: DeepXWsPublicChannel::Trades,
                    ..
                } if market.id == 2 => {
                    let executions = trade_stream.ingest(&frame)?;
                    if initial_trade_page {
                        trade_count += executions.len();
                    } else {
                        ensure!(
                            executions.is_empty(),
                            "initial history was emitted as live trades"
                        );
                        initial_trade_page = true;
                    }
                }
                DeepXWsPublicFrame::Data {
                    market,
                    channel: DeepXWsPublicChannel::Orderbook,
                    ..
                } if market.id == 2 => {
                    book_stream.ingest(&frame)?;
                    book_updates += 1;
                    let snapshot = book_stream.snapshot().context("book snapshot absent")?;
                    println!(
                        "market={} sequence={} bids={} asks={}",
                        market.id,
                        snapshot.last_update_id,
                        snapshot.bids.len(),
                        snapshot.asks.len()
                    );
                }
                DeepXWsPublicFrame::Pong { .. } => {}
                _ => anyhow::bail!("unexpected or unconfirmed public frame"),
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("public trade/book data deadline exceeded")??;
    println!("public trades={trade_count} and order-book updates={book_updates} verified");
    if recovery {
        socket.close(None).await?;
        let mut fresh = connect_public()
            .await
            .context("fresh WebSocket upgrade failed")?;
        let mut fresh_buffered = subscribe(&mut fresh, 2, &[DeepXWsPublicChannel::Trades]).await?;
        let frame = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let frame = next_buffered_frame(&mut fresh, &mut fresh_buffered).await?;
                match &frame {
                    DeepXWsPublicFrame::Data {
                        market,
                        channel: DeepXWsPublicChannel::Trades,
                        ..
                    } if market.id == 2 => {
                        let (start, end) = trade_stream.recovery_window(&frame)?;
                        if end > start {
                            break anyhow::Ok(frame);
                        }
                    }
                    DeepXWsPublicFrame::Pong { .. } => {}
                    _ => anyhow::bail!("unexpected or unconfirmed recovery frame"),
                }
            }
        })
        .await
        .context("fresh trade page deadline exceeded")??;
        let (start_ms, end_ms) = trade_stream.recovery_window(&frame)?;
        let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
        let history = tokio::time::timeout(
            Duration::from_secs(30),
            transport.get_perp_trades_history(
                DEEPX_TESTNET_REST_URL,
                &DeepXPerpTradesHistoryRequest {
                    market_id: 2,
                    start_ms,
                    end_ms,
                    page_size: 100,
                    max_pages: 100,
                },
            ),
        )
        .await
        .context("REST recovery history deadline exceeded")??;
        let executions = trade_stream.reconcile_history(&history, &frame)?;
        ensure!(
            !executions.is_empty(),
            "recovery did not observe an actual new execution"
        );
        println!(
            "public reconnect REST reconciliation verified: start={start_ms} end={end_ms} records={} novel={}",
            history.len(),
            executions.len()
        );
        fresh.close(None).await?;
    }
    Ok(())
}

async fn verify_book(
    socket: &mut Socket,
    market_id: u16,
    depth: NonZeroUsize,
    updates: bool,
) -> anyhow::Result<()> {
    let capacity = depth
        .get()
        .checked_mul(2)
        .context("book depth is too large")?;
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let markets = transport.get_perp_markets(DEEPX_TESTNET_REST_URL).await?;
    let market = markets
        .iter()
        .find(|market| market.id == u64::from(market_id))
        .context("market ID is absent from the REST directory")?;
    let request = DeepXWsPublicRequest::SubscribeBook {
        market_id,
        depth,
        price_size: market.order_spec_tick_size,
    };
    let mut buffered = subscribe_request(
        socket,
        request,
        market_id,
        &[DeepXWsPublicChannel::Orderbook],
    )
    .await?;
    let mut stream = DeepXWsBookStream::new(market_id, NonZeroUsize::new(capacity).unwrap());
    let required = if updates { 3 } else { 1 };
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut seen = 0;
        while seen < required {
            let frame = next_buffered_frame(socket, &mut buffered).await?;
            if matches!(frame, DeepXWsPublicFrame::Pong { .. }) {
                continue;
            }
            let DeepXWsPublicFrame::Data {
                market,
                channel: DeepXWsPublicChannel::Orderbook,
                ..
            } = &frame
            else {
                anyhow::bail!("expected public order-book data");
            };
            ensure!(market.id == market_id, "order-book market mismatch");
            let is_snapshot = stream.ingest(&frame)?;
            if seen == 0 {
                ensure!(is_snapshot, "initial order-book update was not a snapshot");
            }
            let book = stream.snapshot().context("order-book snapshot absent")?;
            let (bid_price, bid) = book.bids.last_key_value().context("book has no bid")?;
            let (ask_price, ask) = book.asks.first_key_value().context("book has no ask")?;
            ensure!(
                book.last_update_id > 0
                    && book.engine_time > 0
                    && book.bids.len() <= depth.get()
                    && book.asks.len() <= depth.get()
                    && *bid_price < *ask_price,
                "order-book depth, timestamp, sequence, or spread is invalid"
            );
            println!(
                "book verified: market={market_id} sequence={} timestamp_ms={} snapshot={is_snapshot} bids={} asks={} best_bid={} x {} best_ask={} x {}",
                book.last_update_id,
                book.engine_time,
                book.bids.len(),
                book.asks.len(),
                bid_price,
                bid.qty,
                ask_price,
                ask.qty
            );
            seen += 1;
        }
        anyhow::Ok(())
    })
    .await
    .context("public order-book data deadline exceeded")??;
    Ok(())
}

async fn verify_spot_live_trades(market_name: &str) -> anyhow::Result<()> {
    let transport = DeepXRestTransport::new(Some(30), std::env::var("DEEPX_PROXY_URL").ok())?;
    let markets = transport.get_spot_markets(DEEPX_TESTNET_REST_URL).await?;
    let market = markets
        .iter()
        .find(|market| market.name == market_name)
        .context("Spot market is absent from the REST directory")?;
    let request = DeepXWsSpotTradeRequest::subscribe(market)?;
    let mut stream = DeepXWsSpotTradeStream::new(market, NonZeroUsize::new(1024).unwrap());
    let mut socket = connect_public().await?;
    socket.send(Message::Text(request.to_text().into())).await?;
    let result = tokio::time::timeout(Duration::from_secs(90), async {
        let mut acknowledged = false;
        let mut buffered = Vec::new();
        loop {
            let frame = DeepXWsSpotTradeFrame::parse(&next_text(&mut socket).await?)?;
            match frame {
                DeepXWsSpotTradeFrame::Subscribed {
                    market,
                    subscriptions,
                } => {
                    ensure!(!acknowledged, "duplicate Spot trade acknowledgement");
                    request.validate_ack(&market, &subscriptions)?;
                    acknowledged = true;
                    for frame in buffered.drain(..) {
                        let trades = stream.ingest(&frame)?;
                        if let Some(trade) = trades.last() {
                            println!(
                                "Spot live trades verified: market={market_name} count={} latest_id={}",
                                trades.len(),
                                trade.id
                            );
                            return anyhow::Ok(());
                        }
                    }
                }
                DeepXWsSpotTradeFrame::Data { ref market, .. } => {
                    ensure!(request.matches_market(market), "Spot trade market mismatch");
                    if acknowledged {
                        let trades = stream.ingest(&frame)?;
                        if let Some(trade) = trades.last() {
                            println!(
                                "Spot live trades verified: market={market_name} count={} latest_id={}",
                                trades.len(),
                                trade.id
                            );
                            break anyhow::Ok(());
                        }
                    } else {
                        ensure!(buffered.len() < 32, "Spot pre-acknowledgement buffer overflow");
                        buffered.push(frame);
                    }
                }
                DeepXWsSpotTradeFrame::Pong => {}
                DeepXWsSpotTradeFrame::Error { code, message } => {
                    anyhow::bail!("Spot trade subscription error {code}: {message}");
                }
            }
        }
    })
    .await
    .context("Spot live trade observation deadline exceeded")?;
    socket.close(None).await?;
    result
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    use super::*;

    #[tokio::test]
    async fn account_metrics_retains_data_before_acknowledgement() -> anyhow::Result<()> {
        const SUBACCOUNT: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            ensure!(
                matches!(socket.next().await, Some(Ok(Message::Text(_)))),
                "account metrics subscription was not sent"
            );
            let frame = |channel, data| {
                json!({
                    "type": "data", "channel": channel, "market": {"type": "all"},
                    "data": data, "timestamp": 1,
                })
                .to_string()
            };
            let total = frame(
                "user_total_deposits",
                json!({
                    "address": SUBACCOUNT, "totalDepositsUsd": 1.0,
                    "totalBorrowsUsd": 0.0, "totalBorrowInterestUsd": 0.0,
                }),
            );
            socket.send(Message::Text(total.clone().into())).await?;
            socket.send(Message::Text(total.into())).await?;
            let subscriptions = [
                "user_total_deposits",
                "user_net_value",
                "user_equity",
                "user_portfolio_margin_ratio",
            ]
            .into_iter()
            .map(|channel| json!({"channel": channel, "address": SUBACCOUNT}))
            .collect::<Vec<_>>();
            socket
                .send(Message::Text(
                    json!({
                        "type": "subscribed", "market": {"type": "all"},
                        "subscriptions": subscriptions,
                    })
                    .to_string()
                    .into(),
                ))
                .await?;
            for (channel, data) in [
                (
                    "user_net_value",
                    json!({
                        "address": SUBACCOUNT, "netValueUsd": 1.0,
                        "totalDepositsUsd": 1.0, "totalBorrowsUsd": 0.0,
                        "totalBorrowInterestUsd": 0.0, "unrealizedPnlUsd": 0.0,
                    }),
                ),
                (
                    "user_equity",
                    json!({
                        "subaccount": SUBACCOUNT, "equityUsd": 1.0,
                        "totalDepositsUsd": 1.0, "totalBorrowsUsd": 0.0,
                        "unrealizedPnlUsd": 0.0,
                    }),
                ),
                (
                    "user_portfolio_margin_ratio",
                    json!({
                        "address": SUBACCOUNT, "availableMargin": 1.0,
                        "initCollateral": 1.0, "initMarginReq": 0.0,
                        "maintMarginReq": 0.0, "totalCollateral": 1.0,
                        "totalMaintenanceMarginRequired": 0.0, "totalMarginRequired": 0.0,
                        "marginRatio": null, "uniIMR": null,
                        "exactMargin": {
                            "address": SUBACCOUNT, "availableMargin": "1",
                            "initCollateral": "1", "initMarginReq": "0",
                            "maintMarginReq": "0", "totalCollateral": "1",
                            "totalMaintenanceMarginRequired": "0", "totalMarginRequired": "0",
                            "marginRatio": null, "uniIMR": null,
                        },
                    }),
                ),
            ] {
                socket
                    .send(Message::Text(frame(channel, data).into()))
                    .await?;
            }
            socket.close(None).await?;
            anyhow::Ok(())
        });
        let (mut socket, _) = connect_async(format!("ws://{address}")).await?;
        let request = DeepXWsAccountMetricsRequest::new(SUBACCOUNT)?;
        let metrics = receive_account_metrics(&mut socket, &request).await?;
        ensure!(
            metrics.len() == DeepXWsAccountMetricChannel::ALL.len(),
            "account metrics were not complete"
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn account_balances_retains_snapshot_before_acknowledgement() -> anyhow::Result<()> {
        const SUBACCOUNT: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            ensure!(
                matches!(socket.next().await, Some(Ok(Message::Text(_)))),
                "account subscription was not sent"
            );
            socket
                .send(Message::Text(
                    include_str!("../test_data/websocket/testnet/user_balances.json")
                        .trim()
                        .into(),
                ))
                .await?;
            socket
                .send(Message::Text(
                    format!(
                        r#"{{"type":"subscribed","market":{{"type":"all"}},"subscriptions":[{{"channel":"user_balances","address":"{SUBACCOUNT}"}}],"message":"ok"}}"#
                    )
                    .into(),
                ))
                .await?;
            socket.close(None).await?;
            anyhow::Ok(())
        });
        let (mut socket, _) = connect_async(format!("ws://{address}")).await?;
        verify_balances(&mut socket, SUBACCOUNT).await?;
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn account_balances_rejects_duplicate_acknowledgement() -> anyhow::Result<()> {
        const SUBACCOUNT: &str = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc";
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            ensure!(
                matches!(socket.next().await, Some(Ok(Message::Text(_)))),
                "account subscription was not sent"
            );
            let ack = format!(
                r#"{{"type":"subscribed","market":{{"type":"all"}},"subscriptions":[{{"channel":"user_balances","address":"{SUBACCOUNT}"}}],"message":"ok"}}"#
            );
            socket.send(Message::Text(ack.clone().into())).await?;
            socket.send(Message::Text(ack.into())).await?;
            socket.close(None).await?;
            anyhow::Ok(())
        });
        let (mut socket, _) = connect_async(format!("ws://{address}")).await?;
        let result = verify_balances(&mut socket, SUBACCOUNT).await;
        ensure!(
            result
                .unwrap_err()
                .to_string()
                .contains("duplicate account balance acknowledgement"),
            "duplicate account acknowledgement was accepted"
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn public_prices_rejects_duplicate_acknowledgement() -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            ensure!(
                matches!(socket.next().await, Some(Ok(Message::Text(_)))),
                "public subscription was not sent"
            );
            let ack = r#"{"type":"subscribed","market":{"type":"perp","id":3},"subscriptions":["latest_price","oracle_price","mark_price","funding_rate"],"message":"subscribed"}"#;
            socket.send(Message::Text(ack.into())).await?;
            socket.send(Message::Text(ack.into())).await?;
            socket.close(None).await?;
            anyhow::Ok(())
        });
        let (mut socket, _) = connect_async(format!("ws://{address}")).await?;
        let result = verify_prices(&mut socket).await;
        ensure!(
            result
                .unwrap_err()
                .to_string()
                .contains("unexpected or unconfirmed price-channel frame"),
            "duplicate public acknowledgement was accepted"
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn public_subscription_retains_data_before_acknowledgement() -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let request = socket
                .next()
                .await
                .context("missing subscription request")??;
            ensure!(request.is_text(), "subscription request must be text");
            socket
                .send(Message::Text(
                    r#"{"type":"data","market":{"type":"perp","id":2},"channel":"latest_price","data":100,"timestamp":1}"#
                        .into(),
                ))
                .await?;
            socket
                .send(Message::Text(
                    r#"{"type":"subscribed","market":{"type":"perp","id":2},"subscriptions":["latest_price"],"message":"subscribed"}"#
                        .into(),
                ))
                .await?;
            socket.close(None).await?;
            anyhow::Ok(())
        });
        let (mut socket, _) = connect_async(format!("ws://{address}")).await?;
        let mut buffered = subscribe(&mut socket, 2, &[DeepXWsPublicChannel::LatestPrice]).await?;
        ensure!(
            buffered.len() == 1,
            "pre-acknowledgement data was not buffered"
        );
        let frame = next_buffered_frame(&mut socket, &mut buffered).await?;
        ensure!(
            matches!(
                frame,
                DeepXWsPublicFrame::Data {
                    channel: DeepXWsPublicChannel::LatestPrice,
                    ..
                }
            ),
            "buffered frame channel changed"
        );
        server.await??;
        Ok(())
    }
}
