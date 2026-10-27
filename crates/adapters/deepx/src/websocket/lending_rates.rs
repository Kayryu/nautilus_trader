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

//! Bounded public lending interest-rate observations without yield interpretation.

use std::time::Duration;

use anyhow::{Result, ensure};
pub use deepx_rust_sdk::websocket::lending_rates::{DeepXWsLendingRate, DeepXWsLendingRatesBatch};
use deepx_rust_sdk::websocket::lending_rates::{
    DeepXWsLendingRatesFrame, DeepXWsLendingRatesRequest as SdkLendingRatesRequest,
};
use nautilus_network::transport::Message;

use super::transport::DeepXWsReadConnection;
use crate::http::{DeepXLendingAsset, DeepXLendingInterestRateRecord};

/// One read-only aggregate lending-rate subscription bound to a validated REST directory.
#[derive(Clone, Debug)]
pub struct DeepXWsLendingRatesRequest {
    inner: SdkLendingRatesRequest,
}

impl DeepXWsLendingRatesRequest {
    /// Creates a request using the current validated lending asset directory.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, duplicated, or invalid asset identity.
    pub fn new(assets: &[DeepXLendingAsset]) -> Result<Self> {
        let inner = SdkLendingRatesRequest::new(
            assets
                .iter()
                .map(|asset| (asset.market_id, asset.asset.clone())),
        )?;
        Ok(Self { inner })
    }

    /// Serializes the documented uncompressed subscription.
    #[must_use]
    pub fn to_text(&self) -> String {
        self.inner.to_text()
    }

    fn parse(&self, text: &str) -> Result<DeepXWsLendingRatesFrame> {
        self.inner.parse(text)
    }
}

/// Checks that every observed WebSocket rate has one identical typed REST history record.
///
/// The caller must independently validate REST request scope and history response bounds. The
/// comparison does not establish finality, continuity, or a tolerance for decimal disagreement.
///
/// # Errors
///
/// Returns an error for a missing, duplicate, or conflicting REST record.
pub fn match_lending_rate_batch_to_rest(
    batch: &DeepXWsLendingRatesBatch,
    records: &[DeepXLendingInterestRateRecord],
) -> Result<()> {
    for rate in &batch.rates {
        let mut candidates = records.iter().filter(|record| {
            record.market_id == rate.market_id
                && record.asset.eq_ignore_ascii_case(&rate.asset)
                && record.statistic_time == batch.statistic_time_ms
        });
        let record = candidates
            .next()
            .ok_or_else(|| anyhow::anyhow!("REST lending history omitted the WebSocket bucket"))?;
        ensure!(
            candidates.next().is_none(),
            "REST lending history repeated the WebSocket bucket"
        );
        ensure!(
            record.borrow_apr == rate.borrow_apr && record.supply_apr == rate.supply_apr,
            "REST lending rates conflict with WebSocket bucket market={} asset={} time={}: ws borrow={} supply={}, REST borrow={} supply={}",
            rate.market_id,
            rate.asset,
            batch.statistic_time_ms,
            rate.borrow_apr,
            rate.supply_apr,
            record.borrow_apr,
            record.supply_apr,
        );
    }
    Ok(())
}

/// Reads the first bounded lending-rate batch after an exact acknowledgement.
///
/// This observation does not establish history completeness, compounding, precision, or framework
/// yield semantics.
///
/// # Errors
///
/// Returns an error for transport, acknowledgement, identity, financial value, or deadline failure.
pub async fn observe_first_lending_rates(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    timeout: Duration,
    request: &DeepXWsLendingRatesRequest,
) -> Result<DeepXWsLendingRatesBatch> {
    ensure!(
        !timeout.is_zero(),
        "DeepX lending-rate timeout must be positive"
    );
    let mut connection = DeepXWsReadConnection::connect(network, proxy_url, timeout).await?;
    if let Err(e) = connection.send_lending_rates_request(request).await {
        let _ = connection.close().await;
        return Err(e);
    }
    let result = tokio::time::timeout(timeout, async {
        let mut acknowledged = false;
        loop {
            let message = match connection.receive().await {
                Ok(Some(message)) => message,
                Ok(None) => {
                    anyhow::bail!("DeepX lending-rate stream closed before its first batch")
                }
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
            match message {
                Message::Text(bytes) => match request.parse(std::str::from_utf8(&bytes)?)? {
                    DeepXWsLendingRatesFrame::Subscribed => {
                        ensure!(
                            !acknowledged,
                            "duplicate DeepX lending-rate acknowledgement"
                        );
                        acknowledged = true;
                    }
                    DeepXWsLendingRatesFrame::Rates(batch) => {
                        ensure!(
                            acknowledged,
                            "DeepX lending rates preceded their acknowledgement"
                        );
                        break Ok::<_, anyhow::Error>(batch);
                    }
                    DeepXWsLendingRatesFrame::Pong => {}
                },
                Message::Ping(_) | Message::Pong(_) => {}
                _ => anyhow::bail!("unsupported DeepX lending-rate stream message"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("DeepX lending-rate deadline exceeded")));
    let close = connection.close().await;
    match result {
        Ok(batch) => {
            close?;
            Ok(batch)
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

    const ACK: &str = r#"{"type":"subscribed","market":{"type":"lending"},"subscriptions":["lending_interest_rate"]}"#;
    const FRAME: &str = r#"{"type":"data","channel":"lending_interest_rate","market":{"type":"lending"},"data":{"items":[{"asset":"eth","borrowApr":0.00019907914921343624,"marketId":1,"statisticTime":1791629700000,"supplyApr":1.2127547341372958e-6}],"statisticTime":1791629700000},"timestamp":1791629721075}"#;

    fn request() -> DeepXWsLendingRatesRequest {
        let asset: DeepXLendingAsset = serde_json::from_str(
            r#"{"marketId":1,"asset":"ETH","height":1,"createdAt":"2026-01-01T00:00:00Z"}"#,
        )
        .unwrap();
        DeepXWsLendingRatesRequest::new(&[asset]).unwrap()
    }

    #[rstest]
    fn parses_observed_rate_without_float_conversion() {
        assert!(matches!(
            request().parse(ACK).unwrap(),
            DeepXWsLendingRatesFrame::Subscribed
        ));
        let DeepXWsLendingRatesFrame::Rates(batch) = request().parse(FRAME).unwrap() else {
            panic!("expected lending-rate batch");
        };
        assert_eq!(batch.rates.len(), 1);
        assert_eq!(batch.statistic_time_ms, 1_791_629_700_000);
        assert_eq!(
            batch.rates[0].supply_apr.to_string(),
            "0.0000012127547341372958"
        );
        assert!(batch.rates[0].raw().get().contains("1.2127547341372958e-6"));
    }

    #[rstest]
    #[case("asset", FRAME.replace("\"eth\"", "\"btc\""))]
    #[case("market", FRAME.replace("\"marketId\":1", "\"marketId\":2"))]
    #[case("negative", FRAME.replace("\"borrowApr\":0.00019907914921343624", "\"borrowApr\":-1"))]
    #[case("timestamp", FRAME.replace("\"statisticTime\":1791629700000},\"timestamp\"", "\"statisticTime\":1791629721076},\"timestamp\""))]
    #[case("unaligned", FRAME.replace("1791629700000", "1791629700001"))]
    fn rejects_foreign_or_invalid_rate(#[case] _scenario: &str, #[case] frame: String) {
        assert!(request().parse(&frame).is_err());
    }

    #[rstest]
    fn exact_rest_match_rejects_one_digit_apr_conflict() {
        let DeepXWsLendingRatesFrame::Rates(batch) = request().parse(FRAME).unwrap() else {
            panic!("expected lending-rate batch");
        };
        let matching: DeepXLendingInterestRateRecord = serde_json::from_str(
            r#"{"market_id":1,"asset":"eth","borrow_apr":"0.00019907914921343624","supply_apr":"0.0000012127547341372958","statistic_time":1791629700000}"#,
        )
        .unwrap();
        match_lending_rate_batch_to_rest(&batch, std::slice::from_ref(&matching)).unwrap();
        let mut conflicting = matching.clone();
        conflicting.supply_apr = "0.0000012127547341372959".parse().unwrap();
        assert!(match_lending_rate_batch_to_rest(&batch, &[conflicting]).is_err());
        assert!(match_lending_rate_batch_to_rest(&batch, &[]).is_err());
        assert!(match_lending_rate_batch_to_rest(&batch, &[matching.clone(), matching]).is_err());
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn observer_requires_ack_before_rates(#[case] ack_first: bool) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/internal/v1/ws",
            get(move |upgrade: WebSocketUpgrade| async move {
                upgrade.on_upgrade(move |mut socket| async move {
                    let Some(Ok(ServerMessage::Text(_))) = socket.recv().await else {
                        panic!("expected lending-rate subscription");
                    };
                    if ack_first {
                        let _ = socket.send(ServerMessage::Text(ACK.into())).await;
                    }
                    let _ = socket.send(ServerMessage::Text(FRAME.into())).await;
                })
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let network = crate::config::DeepXNetworkConfig {
            base_url_ws: Some(format!("ws://{address}")),
            ..Default::default()
        };
        let result =
            observe_first_lending_rates(&network, None, Duration::from_secs(2), &request()).await;
        assert_eq!(result.is_ok(), ack_first);
        server.abort();
    }
}
