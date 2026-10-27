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
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
//  the specific language governing permissions and limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Exact conversion of observed public mark, oracle, and funding frames.

use deepx_rust_sdk::websocket::public::{parse_public_funding_rate, parse_public_price};
use nautilus_common::messages::DataEvent;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Data, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate},
    instruments::{Instrument, InstrumentAny},
    types::Price,
};
#[cfg(test)]
use rust_decimal::Decimal;

use crate::websocket::public::{DeepXWsPublicChannel, DeepXWsPublicFrame};

pub(super) fn parse_public_price_event(
    frame: &DeepXWsPublicFrame,
    expected_channel: DeepXWsPublicChannel,
    expected_market_id: u16,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<DataEvent> {
    let DeepXWsPublicFrame::Data {
        market,
        channel,
        data,
        timestamp,
    } = frame
    else {
        anyhow::bail!("DeepX public price parser requires a data frame");
    };
    anyhow::ensure!(
        market.id == expected_market_id && *channel == expected_channel,
        "DeepX public price frame identity mismatch"
    );
    let ts_event = timestamp_ns(*timestamp)?;
    match channel {
        DeepXWsPublicChannel::MarkPrice | DeepXWsPublicChannel::OraclePrice => {
            let value = parse_public_price(data)?;
            let price = Price::from_decimal(value)?;
            anyhow::ensure!(
                price.as_decimal() == value,
                "DeepX public price would lose precision"
            );
            if *channel == DeepXWsPublicChannel::MarkPrice {
                Ok(DataEvent::Data(Data::MarkPrice(MarkPriceUpdate::new(
                    instrument.id(),
                    price,
                    ts_event,
                    ts_init,
                ))))
            } else {
                Ok(DataEvent::Data(Data::IndexPrice(IndexPriceUpdate::new(
                    instrument.id(),
                    price,
                    ts_event,
                    ts_init,
                ))))
            }
        }
        DeepXWsPublicChannel::FundingRate => {
            let funding_rate = parse_public_funding_rate(data, *timestamp)?;
            Ok(DataEvent::FundingRate(FundingRateUpdate::new(
                instrument.id(),
                funding_rate,
                None,
                None,
                ts_event,
                ts_init,
            )))
        }
        _ => anyhow::bail!("DeepX public channel is not a framework price feed"),
    }
}

fn timestamp_ns(timestamp_ms: u64) -> anyhow::Result<UnixNanos> {
    Ok(UnixNanos::from(
        timestamp_ms
            .checked_mul(1_000_000)
            .ok_or_else(|| anyhow::anyhow!("DeepX public price timestamp overflows nanoseconds"))?,
    ))
}

#[cfg(test)]
mod tests {
    use nautilus_model::instruments::stubs::crypto_perpetual_ethusdt;
    use rstest::rstest;

    use super::*;

    fn instrument() -> InstrumentAny {
        InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt())
    }

    fn frame(channel: &str, data: &str, timestamp: u64) -> DeepXWsPublicFrame {
        DeepXWsPublicFrame::parse(&format!(
            r#"{{"type":"data","channel":"{channel}","market":{{"type":"perp","id":3}},"data":{data},"timestamp":{timestamp}}}"#,
        ))
        .unwrap()
    }

    #[rstest]
    #[case(DeepXWsPublicChannel::MarkPrice, "mark_price")]
    #[case(DeepXWsPublicChannel::OraclePrice, "oracle_price")]
    fn parses_exact_price_frames(
        #[case] channel: DeepXWsPublicChannel,
        #[case] wire_channel: &str,
    ) {
        let event = parse_public_price_event(
            &frame(wire_channel, "2384.511399", 1_789_546_201_652),
            channel,
            3,
            &instrument(),
            UnixNanos::from(7),
        )
        .unwrap();
        match event {
            DataEvent::Data(Data::MarkPrice(update)) => {
                assert_eq!(update.value.as_decimal(), Decimal::new(2_384_511_399, 6));
                assert_eq!(update.ts_event.as_millis(), 1_789_546_201_652);
                assert_eq!(update.ts_init, UnixNanos::from(7));
            }
            DataEvent::Data(Data::IndexPrice(update)) => {
                assert_eq!(update.value.as_decimal(), Decimal::new(2_384_511_399, 6));
                assert_eq!(update.ts_event.as_millis(), 1_789_546_201_652);
                assert_eq!(update.ts_init, UnixNanos::from(7));
            }
            event => panic!("unexpected event: {event:?}"),
        }
    }

    #[rstest]
    fn parses_exact_scientific_price_frame() {
        let event = parse_public_price_event(
            &frame("mark_price", "2.384511399e3", 1_789_546_201_652),
            DeepXWsPublicChannel::MarkPrice,
            3,
            &instrument(),
            UnixNanos::from(7),
        )
        .unwrap();
        let DataEvent::Data(Data::MarkPrice(update)) = event else {
            panic!("expected mark price update")
        };
        assert_eq!(update.value.as_decimal(), Decimal::new(2_384_511_399, 6));
    }

    #[rstest]
    fn parses_funding_without_inventing_payment_schedule() {
        let event = parse_public_price_event(
            &frame(
                "funding_rate",
                r#"{"funding_rate":0.000108085430235316,"last_cacl_funding_rate_time":1789542862949,"last_funding_rate_time":1789546196629}"#,
                1_789_546_201_652,
            ),
            DeepXWsPublicChannel::FundingRate,
            3,
            &instrument(),
            UnixNanos::from(9),
        )
        .unwrap();
        let DataEvent::FundingRate(update) = event else {
            panic!("expected funding update")
        };
        assert_eq!(
            update.rate,
            Decimal::from_str_exact("0.000108085430235316").unwrap()
        );
        assert_eq!(update.interval, None);
        assert_eq!(update.next_funding_ns, None);
        assert_eq!(update.ts_event.as_millis(), 1_789_546_201_652);
    }

    #[rstest]
    #[case("precision", "2384.123456789012345678901234", 1_789_546_201_652)]
    #[case(
        "rounded-plain",
        "2384.51000000000000000000000000001",
        1_789_546_201_652
    )]
    #[case(
        "rounded-scientific",
        "2.384510000000000000000000000001e3",
        1_789_546_201_652
    )]
    #[case("zero", "0", 1_789_546_201_652)]
    #[case("string", r#""2384.51""#, 1_789_546_201_652)]
    #[case("overflow", "2384.51", u64::MAX)]
    fn rejects_invalid_price_frames(
        #[case] _scenario: &str,
        #[case] data: &str,
        #[case] timestamp: u64,
    ) {
        assert!(
            parse_public_price_event(
                &frame("mark_price", data, timestamp),
                DeepXWsPublicChannel::MarkPrice,
                3,
                &instrument(),
                UnixNanos::default(),
            )
            .is_err()
        );
    }

    #[rstest]
    #[case(2, DeepXWsPublicChannel::MarkPrice)]
    #[case(3, DeepXWsPublicChannel::OraclePrice)]
    fn rejects_foreign_identity(#[case] market_id: u16, #[case] channel: DeepXWsPublicChannel) {
        assert!(
            parse_public_price_event(
                &frame("mark_price", "2384.51", 1_789_546_201_652),
                channel,
                market_id,
                &instrument(),
                UnixNanos::default(),
            )
            .is_err()
        );
    }
}
