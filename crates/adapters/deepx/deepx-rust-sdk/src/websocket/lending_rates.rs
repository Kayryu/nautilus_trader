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

//! Exact-decimal public lending-rate WebSocket frames.

use std::{borrow::Borrow, collections::BTreeSet};

use anyhow::{Result, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, value::RawValue};

use crate::{decimal::exact_decimal, rest::models::DeepXLendingInterestRateRecord};

/// One read-only aggregate lending-rate subscription bound to a validated asset directory.
#[derive(Clone, Debug)]
pub struct DeepXWsLendingRatesRequest {
    assets: BTreeSet<(u64, String)>,
}

impl DeepXWsLendingRatesRequest {
    /// Creates a request from lending market IDs and asset symbols.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, duplicated, or invalid asset identity.
    pub fn new(assets: impl IntoIterator<Item = (u64, String)>) -> Result<Self> {
        let mut expected = BTreeSet::new();
        for (market_id, asset) in assets {
            ensure!(
                market_id != 0 && !asset.is_empty(),
                "invalid DeepX lending asset identity"
            );
            ensure!(
                expected.insert((market_id, asset.to_ascii_lowercase())),
                "duplicate DeepX lending asset identity"
            );
        }
        ensure!(
            !expected.is_empty(),
            "DeepX lending asset directory is empty"
        );
        Ok(Self { assets: expected })
    }

    /// Serializes the documented uncompressed subscription.
    #[must_use]
    pub fn to_text(&self) -> String {
        json!({
            "action": "subscribe",
            "market": {"type": "lending"},
            "subscriptions": ["lending_interest_rate"],
            "options": {"compress": false},
        })
        .to_string()
    }

    /// Parses a frame against this request's exact asset directory.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, foreign, or financially invalid frames.
    pub fn parse(&self, text: &str) -> Result<DeepXWsLendingRatesFrame> {
        parse_lending_rates_frame(text, &self.assets)
    }
}

/// One exact-decimal lending APR observation with its original JSON.
#[derive(Debug)]
pub struct DeepXWsLendingRate {
    /// Venue lending market ID.
    pub market_id: u64,
    /// Venue asset symbol.
    pub asset: String,
    /// Exact borrow APR as sent by the venue.
    pub borrow_apr: Decimal,
    /// Exact supply APR as sent by the venue.
    pub supply_apr: Decimal,
    raw: Box<RawValue>,
}

impl DeepXWsLendingRate {
    /// Returns the original item JSON, preserving decimal lexemes.
    #[must_use]
    pub fn raw(&self) -> &RawValue {
        &self.raw
    }
}

/// One mutable lending-rate batch with venue millisecond timestamps.
#[derive(Debug)]
pub struct DeepXWsLendingRatesBatch {
    /// Validated items in venue order.
    pub rates: Vec<DeepXWsLendingRate>,
    /// Venue statistic bucket timestamp.
    pub statistic_time_ms: u64,
    /// WebSocket envelope timestamp.
    pub timestamp_ms: u64,
}

/// Checks that every observed WebSocket rate has one identical REST history record.
///
/// The caller independently validates REST request scope and history bounds. This comparison
/// does not establish finality, continuity, or a tolerance for decimal disagreement.
///
/// # Errors
///
/// Returns an error for a missing, duplicate, or conflicting REST record.
pub fn match_lending_rate_batch_to_rest<R>(
    batch: &DeepXWsLendingRatesBatch,
    records: &[R],
) -> Result<()>
where
    R: Borrow<DeepXLendingInterestRateRecord>,
{
    for rate in &batch.rates {
        let mut candidates = records.iter().filter(|candidate| {
            let record: &DeepXLendingInterestRateRecord = (*candidate).borrow();
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
        let record: &DeepXLendingInterestRateRecord = record.borrow();
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

/// A lending-rate subscription acknowledgement, batch, or heartbeat.
#[derive(Debug)]
pub enum DeepXWsLendingRatesFrame {
    Subscribed,
    Rates(DeepXWsLendingRatesBatch),
    Pong,
}

fn parse_lending_rates_frame(
    text: &str,
    assets: &BTreeSet<(u64, String)>,
) -> Result<DeepXWsLendingRatesFrame> {
    #[derive(Deserialize)]
    struct Market {
        #[serde(rename = "type")]
        kind: String,
    }
    #[derive(Deserialize)]
    struct Frame {
        #[serde(rename = "type")]
        kind: String,
        market: Option<Market>,
        subscriptions: Option<Vec<String>>,
        channel: Option<String>,
        data: Option<Box<RawValue>>,
        timestamp: Option<u64>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Data {
        items: Vec<Box<RawValue>>,
        statistic_time: u64,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Item {
        asset: String,
        #[serde(deserialize_with = "exact_decimal::deserialize")]
        borrow_apr: Decimal,
        market_id: u64,
        statistic_time: u64,
        #[serde(deserialize_with = "exact_decimal::deserialize")]
        supply_apr: Decimal,
    }

    let frame: Frame = serde_json::from_str(text)?;
    if frame.kind == "pong" {
        return Ok(DeepXWsLendingRatesFrame::Pong);
    }
    ensure!(
        frame.market.is_some_and(|market| market.kind == "lending"),
        "foreign DeepX lending market"
    );
    match frame.kind.as_str() {
        "subscribed" => {
            ensure!(
                frame.subscriptions.as_deref() == Some(&["lending_interest_rate".to_string()]),
                "foreign DeepX lending-rate acknowledgement"
            );
            Ok(DeepXWsLendingRatesFrame::Subscribed)
        }
        "data" => {
            ensure!(
                frame.channel.as_deref() == Some("lending_interest_rate"),
                "foreign DeepX lending-rate channel"
            );
            let raw = frame
                .data
                .ok_or_else(|| anyhow::anyhow!("missing DeepX lending-rate data"))?;
            let data: Data = serde_json::from_str(raw.get())?;
            ensure!(
                !data.items.is_empty()
                    && data.items.len() <= assets.len()
                    && valid_timestamp(data.statistic_time)
                    && data.statistic_time.is_multiple_of(60_000),
                "invalid DeepX lending-rate batch"
            );
            let timestamp_ms = frame
                .timestamp
                .ok_or_else(|| anyhow::anyhow!("missing DeepX lending-rate timestamp"))?;
            ensure!(
                valid_timestamp(timestamp_ms) && data.statistic_time <= timestamp_ms,
                "invalid DeepX lending-rate frame timestamp"
            );
            let mut seen = BTreeSet::new();
            let mut rates = Vec::with_capacity(data.items.len());
            for raw in data.items {
                let item: Item = serde_json::from_str(raw.get())?;
                let key = (item.market_id, item.asset.to_ascii_lowercase());
                ensure!(
                    assets.contains(&key)
                        && seen.insert(key)
                        && item.statistic_time == data.statistic_time
                        && item.borrow_apr >= Decimal::ZERO
                        && item.supply_apr >= Decimal::ZERO,
                    "foreign or invalid DeepX lending-rate item"
                );
                rates.push(DeepXWsLendingRate {
                    market_id: item.market_id,
                    asset: item.asset,
                    borrow_apr: item.borrow_apr,
                    supply_apr: item.supply_apr,
                    raw,
                });
            }
            Ok(DeepXWsLendingRatesFrame::Rates(DeepXWsLendingRatesBatch {
                rates,
                statistic_time_ms: data.statistic_time,
                timestamp_ms,
            }))
        }
        _ => anyhow::bail!("unsupported DeepX lending-rate frame"),
    }
}

fn valid_timestamp(timestamp: u64) -> bool {
    timestamp > 0 && i64::try_from(timestamp).is_ok()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const ACK: &str = r#"{"type":"subscribed","market":{"type":"lending"},"subscriptions":["lending_interest_rate"]}"#;
    const FRAME: &str = r#"{"type":"data","channel":"lending_interest_rate","market":{"type":"lending"},"data":{"items":[{"asset":"eth","borrowApr":0.00019907914921343624,"marketId":1,"statisticTime":1791629700000,"supplyApr":1.2127547341372958e-6}],"statisticTime":1791629700000},"timestamp":1791629721075}"#;

    fn request() -> DeepXWsLendingRatesRequest {
        DeepXWsLendingRatesRequest::new([(1, "ETH".to_string())]).unwrap()
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
    fn request_requires_distinct_valid_assets() {
        assert!(DeepXWsLendingRatesRequest::new([]).is_err());
        assert!(DeepXWsLendingRatesRequest::new([(0, "ETH".to_string())]).is_err());
        assert!(DeepXWsLendingRatesRequest::new([(1, String::new())]).is_err());
        let duplicate =
            DeepXWsLendingRatesRequest::new([(1, "ETH".to_string()), (1, "eth".to_string())]);
        assert!(duplicate.is_err());
        let request = DeepXWsLendingRatesRequest::new([(1, "ETH".to_string())]).unwrap();
        assert!(request.to_text().contains("lending_interest_rate"));
    }

    #[rstest]
    fn exact_rest_match_rejects_one_digit_apr_conflict() {
        let request = DeepXWsLendingRatesRequest::new([(1, "ETH".to_string())]).unwrap();
        let DeepXWsLendingRatesFrame::Rates(batch) = request.parse(FRAME).unwrap() else {
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
        assert!(
            match_lending_rate_batch_to_rest::<DeepXLendingInterestRateRecord>(&batch, &[])
                .is_err()
        );
        assert!(match_lending_rate_batch_to_rest(&batch, &[matching.clone(), matching]).is_err());
    }
}
