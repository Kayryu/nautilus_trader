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

//! Historical sampled funding rates; bucket interval is not a payment interval.

use anyhow::{Result, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::{data::FundingRateUpdate, identifiers::InstrumentId};

use crate::http::DeepXFundingRateRecord;

pub(super) fn parse_funding_sample(
    record: &DeepXFundingRateRecord,
    instrument_id: InstrumentId,
    ts_init: UnixNanos,
) -> Result<FundingRateUpdate> {
    ensure!(
        record.time.is_multiple_of(60_000),
        "DeepX funding sample is not a UTC minute bucket"
    );
    let nanos = record
        .time
        .checked_mul(1_000_000)
        .ok_or_else(|| anyhow::anyhow!("DeepX funding bucket timestamp overflows UnixNanos"))?;
    Ok(FundingRateUpdate::new(
        instrument_id,
        record.funding_rate,
        None,
        None,
        UnixNanos::from(nanos),
        ts_init,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("-0.000012500000000000001")]
    #[case("0")]
    #[case("0.1234567890123456789012345678")]
    fn preserves_exact_sample_without_inventing_payment_schedule(#[case] rate: &str) {
        let record = DeepXFundingRateRecord {
            funding_rate: rate.parse().unwrap(),
            time: 60_000,
        };
        let id = InstrumentId::from("ETH-USDC-PERP.DEEPX");
        let sample = parse_funding_sample(&record, id, UnixNanos::from(23u64)).unwrap();
        assert_eq!(sample.instrument_id, id);
        assert_eq!(sample.rate, record.funding_rate);
        assert_eq!(sample.ts_event, UnixNanos::from(60_000_000_000u64));
        assert_eq!(sample.ts_init, UnixNanos::from(23u64));
        assert_eq!(sample.interval, None);
        assert_eq!(sample.next_funding_ns, None);
    }

    #[rstest]
    #[case(60_001)]
    #[case(u64::MAX / 60_000 * 60_000)]
    fn rejects_invalid_or_unrepresentable_bucket_time(#[case] time: u64) {
        let record = DeepXFundingRateRecord {
            funding_rate: "0".parse().unwrap(),
            time,
        };
        assert!(
            parse_funding_sample(
                &record,
                InstrumentId::from("ETH-USDC-PERP.DEEPX"),
                UnixNanos::default()
            )
            .is_err()
        );
    }
}
