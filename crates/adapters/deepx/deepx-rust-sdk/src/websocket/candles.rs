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

//! One-minute perpetual candle protocol state.

use anyhow::{Result, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, value::RawValue};

use super::public::DeepXWsPerpMarket;
use crate::{decimal::exact_decimal, rest::models::DeepXPerpCandle};

const ONE_MINUTE_MS: u64 = 60_000;

#[derive(Deserialize)]
struct CandlePage {
    pair: String,
    details: Vec<WireCandle>,
}

#[derive(Deserialize)]
struct WireCandle {
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    volume: Decimal,
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    high: Decimal,
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    low: Decimal,
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    open: Decimal,
    #[serde(deserialize_with = "exact_decimal::deserialize")]
    close: Decimal,
    time: u64,
}

/// A raw candle superseded by a newer bucket, with an explicit missing-bucket indicator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeepXWsSupersededCandle {
    /// Last observed update for the preceding bucket on this connection.
    pub candle: DeepXPerpCandle,
    /// True when the next observed bucket is not the immediate successor.
    pub gap_after: bool,
}

/// Connection-local state for one market's one-minute trade candles.
#[derive(Debug)]
pub struct DeepXWsCandleStream {
    market_id: u16,
    pair: String,
    acknowledged: bool,
    pending: Option<DeepXPerpCandle>,
    last_timestamp_ms: Option<u64>,
    failed: bool,
}

impl DeepXWsCandleStream {
    /// Creates a stream bound to one perpetual market and its expected pair name.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid market identity.
    pub fn new(market_id: u16, pair: impl Into<String>) -> Result<Self> {
        let pair = pair.into();
        ensure!(
            market_id != 0 && !pair.is_empty(),
            "invalid DeepX candle market identity"
        );
        Ok(Self {
            market_id,
            pair,
            acknowledged: false,
            pending: None,
            last_timestamp_ms: None,
            failed: false,
        })
    }

    /// Serializes the documented read-only one-minute candle subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn subscription_text(&self) -> Result<String> {
        Ok(serde_json::json!({
            "action": "subscribe",
            "market": {"type": "perp", "id": self.market_id},
            "subscriptions": [{"channel": "candles", "time_frame": "1m"}],
            "options": {"compress": false},
        })
        .to_string())
    }

    /// Applies one server frame and returns a candle only when a newer bucket is observed.
    ///
    /// An acknowledgement containing only `time_frame` is provisional: the subsequent data
    /// frame must also bind the market, channel, interval, and pair. A stream is connection-local;
    /// after disconnect, create a new instance rather than carrying an incomplete bucket across.
    ///
    /// # Errors
    ///
    /// Returns an error for conflicting identity, malformed data, or out-of-order buckets.
    /// Any error makes this stream terminal; a new connection needs a new instance.
    pub fn apply(&mut self, text: &str) -> Result<Option<DeepXWsSupersededCandle>> {
        ensure!(!self.failed, "DeepX candle stream is terminal");
        let result = self.apply_inner(text);
        if result.is_err() {
            self.failed = true;
            self.pending = None;
        }
        result
    }

    fn apply_inner(&mut self, text: &str) -> Result<Option<DeepXWsSupersededCandle>> {
        #[derive(Deserialize)]
        struct Frame {
            #[serde(rename = "type")]
            kind: String,
            market: Option<DeepXWsPerpMarket>,
            subscriptions: Option<Vec<Value>>,
            channel: Option<String>,
            interval: Option<String>,
            data: Option<Box<RawValue>>,
            timestamp: Option<u64>,
        }

        let frame: Frame = serde_json::from_str(text)?;
        if frame.kind == "pong" {
            return Ok(None);
        }
        let market = frame
            .market
            .ok_or_else(|| anyhow::anyhow!("missing DeepX candle market"))?;
        ensure!(
            market.kind == "perp" && market.id == self.market_id,
            "foreign DeepX candle market"
        );
        match frame.kind.as_str() {
            "subscribed" => {
                ensure!(!self.acknowledged, "duplicate DeepX candle acknowledgement");
                let subscriptions = frame
                    .subscriptions
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX candle subscriptions"))?;
                ensure!(
                    subscriptions.len() == 1
                        && (subscriptions[0] == serde_json::json!({"time_frame": "1m"})
                            || subscriptions[0]
                                == serde_json::json!({"channel": "candles", "time_frame": "1m"})),
                    "conflicting DeepX candle acknowledgement"
                );
                self.acknowledged = true;
                Ok(None)
            }
            "data" => {
                ensure!(self.acknowledged, "unacknowledged DeepX candle data");
                ensure!(
                    frame.channel.as_deref() == Some("candles")
                        && frame.interval.as_deref() == Some("1m"),
                    "foreign DeepX candle channel or interval"
                );
                let timestamp = frame
                    .timestamp
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX candle timestamp"))?;
                ensure!(
                    self.last_timestamp_ms
                        .is_none_or(|previous| timestamp >= previous),
                    "DeepX candle update timestamp moved backwards"
                );
                let data = frame
                    .data
                    .ok_or_else(|| anyhow::anyhow!("missing DeepX candle data"))?;
                let page: CandlePage = serde_json::from_str(data.get())?;
                ensure!(page.pair == self.pair, "foreign DeepX candle pair");
                ensure!(
                    page.details.len() == 1,
                    "DeepX candle update must contain one bucket"
                );
                let wire = page
                    .details
                    .into_iter()
                    .next()
                    .expect("checked single candle");
                let candle = DeepXPerpCandle {
                    volume: wire.volume,
                    high: wire.high,
                    low: wire.low,
                    open: wire.open,
                    close: wire.close,
                    time: wire.time,
                };
                ensure!(
                    candle.time > 0
                        && candle.time.is_multiple_of(ONE_MINUTE_MS)
                        && timestamp >= candle.time
                        && candle.open > Decimal::ZERO
                        && candle.high >= candle.open.max(candle.close)
                        && candle.low <= candle.open.min(candle.close)
                        && candle.low > Decimal::ZERO
                        && candle.volume >= Decimal::ZERO,
                    "invalid DeepX candle values or time"
                );
                if let Some(previous) = &self.pending {
                    ensure!(
                        candle.time >= previous.time,
                        "DeepX candle bucket moved backwards"
                    );
                }
                self.last_timestamp_ms = Some(timestamp);
                let Some(previous) = self.pending.replace(candle.clone()) else {
                    return Ok(None);
                };
                if candle.time == previous.time {
                    return Ok(None);
                }
                Ok(Some(DeepXWsSupersededCandle {
                    gap_after: candle.time - previous.time != ONE_MINUTE_MS,
                    candle: previous,
                }))
            }
            _ => anyhow::bail!("unsupported DeepX candle frame"),
        }
    }
}
