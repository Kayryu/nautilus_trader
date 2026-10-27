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

//! Bounded public perpetual open-interest observations without unit interpretation.

use std::{num::NonZeroUsize, time::Duration};

use anyhow::{Result, ensure};
use deepx_rust_sdk::websocket::open_interest::parse_open_interest;
pub use deepx_rust_sdk::websocket::open_interest::{
    DeepXWsOpenInterestObservation, match_open_interest_to_rest,
};

use super::public::{DeepXWsPublicChannel, DeepXWsPublicConnection};
#[cfg(test)]
use crate::http::{DeepXOpenInterestPage, DeepXOpenInterestRecord};
#[cfg(test)]
use crate::websocket::public::DeepXWsPublicFrame;

const PRE_ACK_BUFFER_CAPACITY: NonZeroUsize = NonZeroUsize::new(8).expect("nonzero capacity");

/// Reads the first acknowledged open-interest update for one validated perpetual market.
///
/// This is a mutable public observation, not a complete history or a framework event.
///
/// # Errors
///
/// Returns an error for invalid identity or deadlines, transport or acknowledgement failure,
/// foreign frames, invalid values, or a missing first update.
pub async fn observe_first_open_interest(
    network: &crate::config::DeepXNetworkConfig,
    proxy_url: Option<&str>,
    operation_timeout: Duration,
    observation_timeout: Duration,
    market_id: u16,
    pair: &str,
) -> Result<DeepXWsOpenInterestObservation> {
    ensure!(
        market_id != 0 && !pair.is_empty(),
        "invalid DeepX open-interest market"
    );
    ensure!(
        !operation_timeout.is_zero() && !observation_timeout.is_zero(),
        "DeepX open-interest timeouts must be positive"
    );
    let mut connection = DeepXWsPublicConnection::connect(
        network,
        proxy_url,
        operation_timeout,
        PRE_ACK_BUFFER_CAPACITY,
    )
    .await?;
    if let Err(e) = connection
        .subscribe(market_id, vec![DeepXWsPublicChannel::OpenInterest])
        .await
    {
        let _ = connection.close().await;
        return Err(e);
    }
    let result = tokio::time::timeout(observation_timeout, async {
        loop {
            let frame = match connection.next_frame().await {
                Ok(frame) => frame,
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
            if let Some(observation) = parse_open_interest(frame, market_id, pair)? {
                break Ok::<_, anyhow::Error>(observation);
            }
        }
    })
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("DeepX open-interest deadline exceeded")));
    let close = connection.close().await;
    match result {
        Ok(observation) => {
            close?;
            Ok(observation)
        }
        Err(e) => Err(e),
    }
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
