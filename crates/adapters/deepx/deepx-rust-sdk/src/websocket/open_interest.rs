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

//! Exact public open-interest WebSocket observations.

use anyhow::{Result, ensure};
use rust_decimal::Decimal;
use serde_json::value::RawValue;

use super::public::{DeepXWsPublicChannel, DeepXWsPublicFrame};
use crate::rest::models::{DeepXOpenInterestPage, DeepXOpenInterestRecord};

/// One raw open-interest bucket from an acknowledged public subscription.
#[derive(Debug)]
pub struct DeepXWsOpenInterestObservation {
    /// Deployment market ID.
    pub market_id: u16,
    /// Pair name bound to the validated REST market directory.
    pub pair: String,
    /// Exact venue values and statistic bucket.
    pub record: DeepXOpenInterestRecord,
    /// WebSocket envelope timestamp in Unix milliseconds.
    pub timestamp_ms: u64,
    raw: Box<RawValue>,
}

impl DeepXWsOpenInterestObservation {
    /// Returns the original JSON data object, preserving numeric lexemes.
    #[must_use]
    pub fn raw(&self) -> &RawValue {
        &self.raw
    }
}

/// Checks one WebSocket bucket against a separately scoped typed REST response.
///
/// The caller must independently bind the REST request to the same market and bucket. Matching
/// mutable observations does not prove continuity, finality, units, or a framework data mapping.
///
/// # Errors
///
/// Returns an error unless REST contains exactly one identical record for the selected pair.
pub fn match_open_interest_to_rest(
    observation: &DeepXWsOpenInterestObservation,
    rest: &DeepXOpenInterestPage,
) -> Result<()> {
    ensure!(
        rest.pair == observation.pair && rest.details.len() == 1,
        "REST open interest does not contain one matching pair bucket"
    );
    ensure!(
        rest.details[0] == observation.record,
        "REST open interest conflicts with WebSocket market={} pair={} time={}: ws={:?}, REST={:?}",
        observation.market_id,
        observation.pair,
        observation.record.statistic_time,
        observation.record,
        rest.details[0],
    );
    Ok(())
}

/// Parses a public frame into an exact open-interest observation for one market.
///
/// # Errors
///
/// Returns an error for a foreign frame or invalid bucket, timestamp, or value.
pub fn parse_open_interest(
    frame: DeepXWsPublicFrame,
    market_id: u16,
    pair: &str,
) -> Result<Option<DeepXWsOpenInterestObservation>> {
    let DeepXWsPublicFrame::Data {
        market,
        channel,
        data,
        timestamp,
    } = frame
    else {
        ensure!(
            matches!(frame, DeepXWsPublicFrame::Pong { .. }),
            "unexpected DeepX open-interest frame"
        );
        return Ok(None);
    };
    ensure!(
        channel == DeepXWsPublicChannel::OpenInterest
            && market.id == market_id
            && market.name.as_deref() == Some(pair),
        "foreign DeepX open-interest market or channel"
    );
    let record: DeepXOpenInterestRecord = serde_json::from_str(data.get())?;
    ensure!(
        timestamp > 0
            && i64::try_from(timestamp).is_ok()
            && record.statistic_time > 0
            && record.statistic_time.is_multiple_of(60_000)
            && record.statistic_time <= timestamp
            && record.total_oi >= Decimal::ZERO
            && record.long_short_ratio >= Decimal::ZERO,
        "invalid DeepX open-interest time or value"
    );
    Ok(Some(DeepXWsOpenInterestObservation {
        market_id,
        pair: pair.to_string(),
        record,
        timestamp_ms: timestamp,
        raw: data,
    }))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const FRAME: &str = r#"{"type":"data","channel":"open_interest","market":{"type":"perp","id":3,"name":"ETH-USDC"},"data":{"long_position_count":2063,"long_short_ratio":"2.57875","short_position_count":800,"statistic_time":1791632580000,"total_oi":"3323.273"},"timestamp":1791632597660}"#;

    #[rstest]
    fn parses_observed_exact_open_interest() {
        let frame = DeepXWsPublicFrame::parse(FRAME).unwrap();
        let observation = parse_open_interest(frame, 3, "ETH-USDC").unwrap().unwrap();
        assert_eq!(observation.record.total_oi, "3323.273".parse().unwrap());
        assert_eq!(
            observation.record.long_short_ratio,
            "2.57875".parse().unwrap()
        );
        assert_eq!(observation.record.long_position_count, 2063);
        assert_eq!(observation.record.short_position_count, 800);
        assert_eq!(observation.record.statistic_time, 1_791_632_580_000);
        assert!(
            observation
                .raw()
                .get()
                .contains("\"total_oi\":\"3323.273\"")
        );
        match_open_interest_to_rest(
            &observation,
            &DeepXOpenInterestPage {
                pair: "ETH-USDC".to_string(),
                details: vec![observation.record.clone()],
            },
        )
        .unwrap();
    }

    #[rstest]
    #[case("market", FRAME.replace("\"id\":3", "\"id\":4"))]
    #[case("pair", FRAME.replace("ETH-USDC", "BTC-USDC"))]
    #[case("bucket", FRAME.replace("1791632580000", "1791632580001"))]
    #[case("future", FRAME.replace("1791632597660", "1791632579999"))]
    #[case("negative", FRAME.replace("\"3323.273\"", "\"-1\""))]
    #[case("precision", FRAME.replace("\"3323.273\"", "\"3323.27399999999999999999999999999\""))]
    fn rejects_foreign_or_invalid_open_interest(#[case] _scenario: &str, #[case] text: String) {
        let frame = DeepXWsPublicFrame::parse(&text).unwrap();
        assert!(parse_open_interest(frame, 3, "ETH-USDC").is_err());
    }

    #[rstest]
    fn exact_rest_comparison_rejects_changed_same_bucket() {
        let frame = DeepXWsPublicFrame::parse(FRAME).unwrap();
        let observation = parse_open_interest(frame, 3, "ETH-USDC").unwrap().unwrap();
        let changed: DeepXOpenInterestRecord = serde_json::from_str(
            r#"{"long_position_count":2064,"long_short_ratio":"2.583229036295369","short_position_count":799,"statistic_time":1791632580000,"total_oi":"3323.493"}"#,
        )
        .unwrap();
        assert!(
            match_open_interest_to_rest(
                &observation,
                &DeepXOpenInterestPage {
                    pair: "ETH-USDC".to_string(),
                    details: vec![changed],
                },
            )
            .is_err()
        );
    }
}
