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

use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use anyhow::{Result, ensure};
use nautilus_network::transport::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};

use super::{
    account::{DeepXWsAllMarket, DeepXWsUserBalancesRequest},
    transport::DeepXWsReadConnection,
};
use crate::config::DeepXNetworkConfig;

/// Account summary channels with an initial WebSocket value.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DeepXWsAccountMetricChannel {
    /// Deposit and borrow totals.
    TotalDeposits,
    /// Net value summary.
    NetValue,
    /// Equity summary.
    Equity,
    /// Portfolio collateral and margin summary.
    PortfolioMarginRatio,
}

impl DeepXWsAccountMetricChannel {
    const ALL: [Self; 4] = [
        Self::TotalDeposits,
        Self::NetValue,
        Self::Equity,
        Self::PortfolioMarginRatio,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::TotalDeposits => "user_total_deposits",
            Self::NetValue => "user_net_value",
            Self::Equity => "user_equity",
            Self::PortfolioMarginRatio => "user_portfolio_margin_ratio",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|channel| channel.as_str() == value)
            .ok_or_else(|| anyhow::anyhow!("unsupported DeepX account summary channel"))
    }

    fn numeric_fields(self) -> &'static [&'static str] {
        match self {
            Self::TotalDeposits => &[
                "totalDepositsUsd",
                "totalBorrowsUsd",
                "totalBorrowInterestUsd",
            ],
            Self::NetValue => &[
                "netValueUsd",
                "totalDepositsUsd",
                "totalBorrowsUsd",
                "totalBorrowInterestUsd",
                "unrealizedPnlUsd",
            ],
            Self::Equity => &[
                "equityUsd",
                "totalDepositsUsd",
                "totalBorrowsUsd",
                "unrealizedPnlUsd",
            ],
            Self::PortfolioMarginRatio => &[
                "availableMargin",
                "initCollateral",
                "initMarginReq",
                "maintMarginReq",
                "totalCollateral",
                "totalMaintenanceMarginRequired",
                "totalMarginRequired",
            ],
        }
    }
}

/// One exact subaccount subscription for four initial account summaries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsAccountMetricsRequest {
    subaccount: String,
}

impl DeepXWsAccountMetricsRequest {
    /// Creates a read-only subscription for a canonical AccountId20 subaccount.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid address.
    pub fn new(subaccount: &str) -> Result<Self> {
        Ok(Self {
            subaccount: DeepXWsUserBalancesRequest::new(subaccount)?
                .subaccount()
                .to_string(),
        })
    }

    /// Returns the subscribed subaccount.
    #[must_use]
    pub fn subaccount(&self) -> &str {
        &self.subaccount
    }

    /// Serializes the documented uncompressed subscription.
    #[must_use]
    pub fn to_text(&self) -> String {
        let subscriptions: Vec<_> = DeepXWsAccountMetricChannel::ALL
            .into_iter()
            .map(|channel| json!({"channel": channel.as_str(), "address": self.subaccount}))
            .collect();
        json!({
            "action": "subscribe",
            "market": {"type": "all"},
            "subscriptions": subscriptions,
            "options": {"compress": false},
        })
        .to_string()
    }
}

/// A raw account summary bound to its channel and subaccount.
#[derive(Debug)]
pub struct DeepXWsAccountMetric {
    channel: DeepXWsAccountMetricChannel,
    data: Box<RawValue>,
    exact_portfolio_margin: Option<DeepXWsExactPortfolioMargin>,
    timestamp_ms: u64,
}

/// Exact decimal fields from one mutable portfolio-margin WebSocket observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepXWsExactPortfolioMargin {
    /// Observed subaccount address.
    pub subaccount: String,
    /// Available margin in the venue's USD unit.
    pub available_margin: Decimal,
    /// Initial collateral in the venue's USD unit.
    pub init_collateral: Decimal,
    /// Initial margin requirement in the venue's USD unit.
    pub init_margin_req: Decimal,
    /// Maintenance margin requirement in the venue's USD unit.
    pub maint_margin_req: Decimal,
    /// Total collateral in the venue's USD unit.
    pub total_collateral: Decimal,
    /// Total maintenance margin requirement in the venue's USD unit.
    pub total_maintenance_margin_required: Decimal,
    /// Total margin requirement in the venue's USD unit.
    pub total_margin_required: Decimal,
    /// Venue margin ratio, absent when no ratio applies.
    pub margin_ratio: Option<Decimal>,
    /// Venue unified initial margin ratio, absent when no ratio applies.
    pub uni_imr: Option<Decimal>,
}

impl DeepXWsAccountMetric {
    /// Returns the source channel.
    #[must_use]
    pub const fn channel(&self) -> DeepXWsAccountMetricChannel {
        self.channel
    }

    /// Returns the unmodified JSON object, including venue numeric lexemes.
    #[must_use]
    pub fn raw_data(&self) -> &RawValue {
        &self.data
    }

    /// Returns exact portfolio-margin values when this is that channel.
    #[must_use]
    pub const fn exact_portfolio_margin(&self) -> Option<&DeepXWsExactPortfolioMargin> {
        self.exact_portfolio_margin.as_ref()
    }

    /// Returns the venue frame timestamp in Unix milliseconds.
    #[must_use]
    pub const fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }
}

enum Frame {
    Subscribed,
    Metric(Box<DeepXWsAccountMetric>),
    Pong,
}

impl Frame {
    fn parse(text: &str, request: &DeepXWsAccountMetricsRequest) -> Result<Self> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Subscription {
            channel: String,
            address: String,
        }
        #[derive(Deserialize)]
        struct WireFrame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsAllMarket>,
            subscriptions: Option<Vec<Subscription>>,
            channel: Option<String>,
            data: Option<Box<RawValue>>,
            timestamp: Option<u64>,
        }

        let wire: WireFrame = serde_json::from_str(text)?;
        if wire.kind == "pong" {
            return Ok(Self::Pong);
        }
        ensure!(
            wire.market
                .as_ref()
                .is_some_and(|market| market.kind == "all"),
            "foreign DeepX account summary market"
        );
        match wire.kind.as_str() {
            "subscribed" => {
                let subscriptions = wire.subscriptions.ok_or_else(|| {
                    anyhow::anyhow!("missing DeepX account summary subscriptions")
                })?;
                let mut observed = HashSet::new();
                for subscription in subscriptions {
                    let channel = DeepXWsAccountMetricChannel::parse(&subscription.channel)?;
                    ensure!(
                        subscription
                            .address
                            .eq_ignore_ascii_case(&request.subaccount)
                            && observed.insert(channel),
                        "foreign or duplicate DeepX account summary subscription"
                    );
                }
                ensure!(
                    observed.len() == DeepXWsAccountMetricChannel::ALL.len(),
                    "incomplete DeepX account summary acknowledgement"
                );
                Ok(Self::Subscribed)
            }
            "data" => {
                let channel =
                    DeepXWsAccountMetricChannel::parse(wire.channel.as_deref().ok_or_else(
                        || anyhow::anyhow!("missing DeepX account summary channel"),
                    )?)?;
                let data = wire
                    .data
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX account summary data"))?;
                let value: Value = serde_json::from_str(data.get())?;
                let object = value
                    .as_object()
                    .ok_or_else(|| anyhow::anyhow!("invalid DeepX account summary data"))?;
                let owner_field = if channel == DeepXWsAccountMetricChannel::Equity {
                    "subaccount"
                } else {
                    "address"
                };
                ensure!(
                    object
                        .get(owner_field)
                        .and_then(Value::as_str)
                        .is_some_and(|address| address.eq_ignore_ascii_case(&request.subaccount)),
                    "foreign DeepX account summary data"
                );
                ensure!(
                    channel
                        .numeric_fields()
                        .iter()
                        .all(|field| { object.get(*field).is_some_and(Value::is_number) }),
                    "incomplete DeepX account summary data"
                );
                let exact_portfolio_margin = (channel
                    == DeepXWsAccountMetricChannel::PortfolioMarginRatio)
                    .then(|| parse_exact_portfolio_margin(object, &request.subaccount))
                    .transpose()?;
                let timestamp_ms = wire
                    .timestamp
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX account summary timestamp"))?;
                ensure!(
                    timestamp_ms > 0 && i64::try_from(timestamp_ms).is_ok(),
                    "invalid DeepX account summary timestamp"
                );
                Ok(Self::Metric(Box::new(DeepXWsAccountMetric {
                    channel,
                    data,
                    exact_portfolio_margin,
                    timestamp_ms,
                })))
            }
            _ => anyhow::bail!("unsupported DeepX account summary frame"),
        }
    }
}

fn parse_exact_portfolio_margin(
    object: &serde_json::Map<String, Value>,
    subaccount: &str,
) -> Result<DeepXWsExactPortfolioMargin> {
    let exact = object
        .get("exactMargin")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("missing DeepX exact portfolio margin"))?;
    ensure!(
        exact
            .get("address")
            .and_then(Value::as_str)
            .is_some_and(|address| address.eq_ignore_ascii_case(subaccount)),
        "foreign DeepX exact portfolio margin"
    );
    let amount = |field: &str| -> Result<Decimal> {
        let value = exact.get(field).and_then(Value::as_str).ok_or_else(|| {
            anyhow::anyhow!("missing DeepX exact portfolio margin field: {field}")
        })?;
        Ok(Decimal::from_str_exact(value)?)
    };
    let ratio = |field: &str| -> Result<Option<Decimal>> {
        let value = exact.get(field).ok_or_else(|| {
            anyhow::anyhow!("missing DeepX exact portfolio margin field: {field}")
        })?;
        if value.is_null() {
            Ok(None)
        } else {
            Ok(Some(Decimal::from_str_exact(value.as_str().ok_or_else(
                || anyhow::anyhow!("invalid DeepX exact ratio: {field}"),
            )?)?))
        }
    };
    for field in ["marginRatio", "uniIMR"] {
        ensure!(
            object
                .get(field)
                .is_some_and(|value| value.is_null() || value.is_number()),
            "invalid DeepX portfolio margin ratio field: {field}"
        );
    }
    Ok(DeepXWsExactPortfolioMargin {
        subaccount: subaccount.to_string(),
        available_margin: amount("availableMargin")?,
        init_collateral: amount("initCollateral")?,
        init_margin_req: amount("initMarginReq")?,
        maint_margin_req: amount("maintMarginReq")?,
        total_collateral: amount("totalCollateral")?,
        total_maintenance_margin_required: amount("totalMaintenanceMarginRequired")?,
        total_margin_required: amount("totalMarginRequired")?,
        margin_ratio: ratio("marginRatio")?,
        uni_imr: ratio("uniIMR")?,
    })
}

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
                    let key = metric.channel;
                    if let Some(previous) = metrics.get(&key) {
                        ensure!(
                            previous.data.get() == metric.data.get(),
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
    fn parses_observed_summary_shapes_without_rounded_numbers() {
        let request = request();
        let encoded: Value = serde_json::from_str(&request.to_text()).unwrap();
        assert_eq!(encoded["market"]["type"], "all");
        assert_eq!(encoded["subscriptions"].as_array().unwrap().len(), 4);
        assert!(matches!(
            Frame::parse(ACK, &request).unwrap(),
            Frame::Subscribed
        ));
        let Frame::Metric(metric) = Frame::parse(TOTAL, &request).unwrap() else {
            panic!("expected summary frame");
        };
        assert_eq!(metric.channel(), DeepXWsAccountMetricChannel::TotalDeposits);
        assert!(metric.raw_data().get().contains("970.9600"));
        assert!(metric.exact_portfolio_margin().is_none());
        assert_eq!(metric.timestamp_ms(), 1_791_494_312_743);
        assert!(matches!(
            Frame::parse(NET, &request).unwrap(),
            Frame::Metric(_)
        ));
        assert!(matches!(
            Frame::parse(EQUITY, &request).unwrap(),
            Frame::Metric(_)
        ));
        let Frame::Metric(margin) = Frame::parse(MARGIN, &request).unwrap() else {
            panic!("expected portfolio margin frame");
        };
        assert_eq!(
            margin.channel(),
            DeepXWsAccountMetricChannel::PortfolioMarginRatio
        );
        assert!(margin.raw_data().get().contains("\"970.963729\""));
        let exact = margin.exact_portfolio_margin().unwrap();
        assert_eq!(exact.subaccount, ADDRESS);
        assert_eq!(exact.available_margin.to_string(), "970.963729");
        assert_eq!(exact.init_collateral.to_string(), "970.963729");
        assert_eq!(exact.init_margin_req, Decimal::ZERO);
        assert_eq!(exact.maint_margin_req, Decimal::ZERO);
        assert_eq!(exact.total_collateral.to_string(), "970.963729");
        assert_eq!(exact.total_maintenance_margin_required, Decimal::ZERO);
        assert_eq!(exact.total_margin_required, Decimal::ZERO);
        assert_eq!(exact.margin_ratio, None);
        assert_eq!(exact.uni_imr, None);
    }

    #[rstest]
    fn parses_populated_exact_ratios_without_using_rounded_outer_numbers() {
        let mut frame: Value = serde_json::from_str(MARGIN).unwrap();
        frame["data"]["marginRatio"] = json!(0.12);
        frame["data"]["uniIMR"] = json!(0.34);
        frame["data"]["exactMargin"]["marginRatio"] = json!("0.123456789012345678");
        frame["data"]["exactMargin"]["uniIMR"] = json!("0.345678901234567890");
        let Frame::Metric(metric) = Frame::parse(&frame.to_string(), &request()).unwrap() else {
            panic!("expected portfolio margin frame");
        };
        let exact = metric.exact_portfolio_margin().unwrap();
        assert_eq!(
            exact.margin_ratio.unwrap().to_string(),
            "0.123456789012345678"
        );
        assert_eq!(exact.uni_imr.unwrap().to_string(), "0.345678901234567890");
    }

    #[rstest]
    fn rejects_wrong_summary_identity_or_numeric_shape() {
        let foreign = EQUITY.replace(ADDRESS, "0x0000000000000000000000000000000000000000");
        assert!(Frame::parse(&foreign, &request()).is_err());
        let wrong_number = TOTAL.replace("970.9600", "\"970.9600\"");
        assert!(Frame::parse(&wrong_number, &request()).is_err());
        let missing = NET.replace("\"netValueUsd\":970.96,", "");
        assert!(Frame::parse(&missing, &request()).is_err());
        let foreign_market = TOTAL.replace("\"type\":\"all\"", "\"type\":\"perp\"");
        assert!(Frame::parse(&foreign_market, &request()).is_err());
        let mut foreign_exact: Value = serde_json::from_str(MARGIN).unwrap();
        foreign_exact["data"]["exactMargin"]["address"] =
            json!("0x0000000000000000000000000000000000000000");
        assert!(Frame::parse(&foreign_exact.to_string(), &request()).is_err());
        let invalid_exact = MARGIN.replace("\"970.963729\"", "\"not-a-decimal\"");
        assert!(Frame::parse(&invalid_exact, &request()).is_err());
        let mut invalid_ratio: Value = serde_json::from_str(MARGIN).unwrap();
        invalid_ratio["data"]["exactMargin"]["marginRatio"] = json!("not-a-decimal");
        assert!(Frame::parse(&invalid_ratio.to_string(), &request()).is_err());
        let missing_exact = MARGIN.replace("\"exactMargin\":", "\"other\":");
        assert!(Frame::parse(&missing_exact, &request()).is_err());
    }

    #[rstest]
    #[case("availableMargin")]
    #[case("marginRatio")]
    fn rejects_inexact_portfolio_margin_decimals(#[case] field: &str) {
        let mut frame: Value = serde_json::from_str(MARGIN).unwrap();
        frame["data"]["exactMargin"][field] = json!("0.12345678901234567890123456789");
        assert!(Frame::parse(&frame.to_string(), &request()).is_err());
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
