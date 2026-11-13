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

//! Conditional Spot sandwich quotes from hash-pinned native order-book state.

use std::collections::BTreeMap;

use alloy_primitives::U256;
use anyhow::Context;
use deepx_rust_sdk::rpc::{DeepXRpcClient, storage_decode::spot_market_storage_key};
use parity_scale_codec::{Decode, DecodeAll, Encode};
use serde_json::json;
use subxt_core::{
    Metadata,
    ext::scale_value::{At, Value, scale::decode_as_type},
};

const DEPTH: u32 = 4_096;
const FEE_SCALE: u128 = 1_000_000;

pub(super) struct Settings {
    pub(super) maximum_front_size: u128,
    pub(super) minimum_profit: u128,
    pub(super) cost_budget: u128,
}

pub(super) struct Snapshot {
    pub(super) base_scale: u128,
    pub(super) minimum_size: u128,
    pub(super) step: u128,
    pub(super) tick: u128,
    pub(super) minimum_notional: u128,
    pub(super) taker_fee: u128,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

struct Level {
    price: u128,
    size: u128,
}

#[derive(Decode)]
struct RawLevel {
    price: [u8; 32],
    size: [u8; 32],
    order_count: u32,
}

#[derive(Debug)]
pub(super) struct Plan {
    /// True when the target sells, so the entry sells inventory and the exit buys it back.
    pub(super) entry_is_sell: bool,
    pub(super) front_size: u128,
    pub(super) front_price: u128,
    /// Maximum spend of a buy entry, or minimum gross proceeds of a sell entry.
    pub(super) front_quote_budget: u128,
    pub(super) back_size: u128,
    pub(super) back_price: u128,
    /// Minimum net proceeds of a sell exit, or maximum cost of a buy exit.
    pub(super) back_quote_bound: u128,
    minimum_profit: u128,
}

impl Plan {
    /// Whether this fresh plan still covers an entry already signed with `entry_quote`.
    pub(super) fn covers_entry(
        &self,
        entry_quote: u128,
        taker_fee: u128,
        cost_budget: u128,
        minimum_profit: u128,
    ) -> anyhow::Result<bool> {
        let margin = cost_budget
            .checked_add(minimum_profit)
            .context("entry profit bound overflow")?;
        if self.entry_is_sell {
            let fee = multiply_divide_ceil(entry_quote, taker_fee, FEE_SCALE)?;
            let net = entry_quote
                .checked_sub(fee)
                .context("entry fee exceeds proceeds")?;
            let required = self
                .back_quote_bound
                .checked_add(margin)
                .context("entry profit bound overflow")?;
            Ok(self.front_quote_budget >= entry_quote && net >= required)
        } else {
            let required = entry_quote
                .checked_add(margin)
                .context("entry profit bound overflow")?;
            Ok(self.back_quote_bound >= required && self.front_quote_budget <= entry_quote)
        }
    }
}

impl Snapshot {
    pub(super) fn executable_exit_size(&self, size: u128) -> anyhow::Result<u128> {
        aligned_size(size, self.tick, self.base_scale, self.step)
    }

    /// Gross buy size whose net base after the taker fee bound covers `net`, rounded up to an exact lot.
    pub(super) fn buy_back_size(&self, net: u128) -> anyhow::Result<u128> {
        anyhow::ensure!(
            net > 0 && self.taker_fee < FEE_SCALE,
            "invalid buy-back quantity or fee"
        );
        let mut gross = multiply_divide_ceil(net, FEE_SCALE, FEE_SCALE - self.taker_fee)?;
        while gross
            .checked_sub(multiply_divide_ceil(gross, self.taker_fee, FEE_SCALE)?)
            .context("buy-back fee exceeds size")?
            < net
        {
            gross = gross.checked_add(1).context("buy-back size overflow")?;
        }
        let lot = exact_lot(self.tick, self.base_scale, self.step)?;
        let round_up = |size: u128| -> anyhow::Result<u128> {
            size.div_ceil(lot)
                .checked_mul(lot)
                .context("buy-back lot overflow")
        };
        Ok(round_up(gross)?.max(round_up(self.minimum_size)?))
    }

    pub(super) fn has_bid(&self, minimum_price: u128) -> bool {
        self.bids
            .first()
            .is_some_and(|bid| bid.price >= minimum_price)
    }

    pub(super) fn has_ask(&self, maximum_price: u128) -> bool {
        self.asks
            .first()
            .is_some_and(|ask| ask.price <= maximum_price)
    }

    pub(super) fn analyze(
        &self,
        order: &serde_json::Value,
        settings: &Settings,
    ) -> serde_json::Value {
        let result = self.plan(order, settings);
        match result {
            Ok(plan) => {
                let bound_key = if plan.entry_is_sell {
                    "maximum_back_quote_cost"
                } else {
                    "minimum_back_quote_proceeds"
                };
                let mut analysis = json!({
                    "conditional_opportunity": true,
                    "execution_ready": false,
                    "entry_side": if plan.entry_is_sell { "sell" } else { "buy" },
                    "front_raw_base_size": plan.front_size.to_string(),
                    "front_raw_limit_price": plan.front_price.to_string(),
                    "front_raw_quote_budget": plan.front_quote_budget.to_string(),
                    "back_raw_base_size": plan.back_size.to_string(),
                    "back_raw_limit_price": plan.back_price.to_string(),
                    "conditional_minimum_profit_quote": plan.minimum_profit.to_string(),
                    "requires": "front fill before target, target residual opposite quote, unchanged execution state",
                });
                analysis[bound_key] = json!(plan.back_quote_bound.to_string());
                analysis
            }
            Err(e) => json!({"conditional_opportunity": false, "execution_ready": false,
                "reason": e.to_string(),
                "best_ask_raw_price": self.asks.first().map(|level| level.price.to_string()),
                "best_bid_raw_price": self.bids.first().map(|level| level.price.to_string()),
                "base_scale": self.base_scale.to_string()}),
        }
    }

    pub(super) fn plan(
        &self,
        order: &serde_json::Value,
        settings: &Settings,
    ) -> anyhow::Result<Plan> {
        let target_is_sell = order["side"] == "sell";
        anyhow::ensure!(
            (order["side"] == "buy" || target_is_sell)
                && order["order_type"] == "Limit"
                && order["time_in_force"] == "GTC"
                && order["post_only"] == "None"
                && order["reduce_only"] == false,
            "target must be an unrestricted GTC limit order with a possible residual quote"
        );
        let base: u128 = order["raw_base_size"]
            .as_str()
            .context("missing target size")?
            .parse()?;
        let quote: u128 = order["raw_quote_amount"]
            .as_str()
            .context("missing target quote")?
            .parse()?;
        anyhow::ensure!(
            base > 0 && quote > 0 && self.step > 0 && self.tick > 0,
            "invalid order or market units"
        );
        anyhow::ensure!(
            base.is_multiple_of(self.step),
            "target size requires runtime normalization"
        );
        let numerator = U256::from(quote) * U256::from(self.base_scale);
        anyhow::ensure!(
            numerator % U256::from(base) == U256::ZERO,
            "target price is inexact"
        );
        let target_price = narrow(numerator / U256::from(base))?;
        anyhow::ensure!(
            target_price > 0 && target_price % self.tick == 0,
            "target price requires runtime normalization"
        );
        if target_is_sell {
            return self.plan_sell(base, target_price, settings);
        }
        anyhow::ensure!(
            self.asks.len() < DEPTH as usize
                || self
                    .asks
                    .last()
                    .is_some_and(|level| level.price > target_price),
            "order-book depth is truncated at the target price"
        );

        let consumed = self
            .asks
            .iter()
            .take_while(|level| level.price <= target_price)
            .try_fold(0_u128, |sum, level| {
                sum.checked_add(level.size)
                    .context("depth quantity overflow")
            })?;
        anyhow::ensure!(
            consumed > 0,
            "target limit price {target_price} does not cross visible asks"
        );
        anyhow::ensure!(
            base >= consumed,
            "target raw base {base} is below visible ask quantity {consumed} at or below limit price {target_price}"
        );
        let maximum = settings.maximum_front_size.min(consumed);
        let mut available = 0_u128;
        let mut best: Option<Plan> = None;

        for level in self
            .asks
            .iter()
            .take_while(|level| level.price <= target_price)
        {
            available = available
                .checked_add(level.size)
                .context("depth quantity overflow")?;
            let front_size = aligned_size(
                available.min(maximum),
                level.price,
                self.base_scale,
                self.step,
            )?;
            if front_size < self.minimum_size {
                continue;
            }

            // Buys pay fees in received base, so the sell leg cannot use gross entry size
            let base_fee_bound = multiply_divide_ceil(front_size, self.taker_fee, FEE_SCALE)?;
            let net_base = front_size
                .checked_sub(base_fee_bound)
                .context("entry fee exceeds size")?;
            let back_size = self.executable_exit_size(net_base)?;
            if back_size < self.minimum_size {
                continue;
            }
            let front_quote_budget =
                multiply_divide_ceil(front_size, level.price, self.base_scale)?;
            let back_quote = multiply_divide_floor(back_size, target_price, self.base_scale)?;
            if front_quote_budget < self.minimum_notional || back_quote < self.minimum_notional {
                continue;
            }
            let quote_fee_bound = multiply_divide_ceil(back_quote, self.taker_fee, FEE_SCALE)?;
            let minimum_back_proceeds = back_quote
                .checked_sub(quote_fee_bound)
                .context("exit fee exceeds proceeds")?;
            let costs = front_quote_budget
                .checked_add(settings.cost_budget)
                .context("cost budget overflow")?;
            if let Some(minimum_profit) = minimum_back_proceeds.checked_sub(costs)
                && minimum_profit > 0
                && minimum_profit >= settings.minimum_profit
                && best
                    .as_ref()
                    .is_none_or(|best| minimum_profit > best.minimum_profit)
            {
                best = Some(Plan {
                    entry_is_sell: false,
                    front_size,
                    front_price: level.price,
                    front_quote_budget,
                    back_size,
                    back_price: target_price,
                    back_quote_bound: minimum_back_proceeds,
                    minimum_profit,
                });
            }
            if available >= maximum {
                break;
            }
        }
        best.context("no positive conditional profit after fee and cost bounds")
    }

    /// Sell target: sell into bids at or above its limit, then buy back from its residual ask.
    fn plan_sell(
        &self,
        base: u128,
        target_price: u128,
        settings: &Settings,
    ) -> anyhow::Result<Plan> {
        anyhow::ensure!(
            self.bids.len() < DEPTH as usize
                || self
                    .bids
                    .last()
                    .is_some_and(|level| level.price < target_price),
            "order-book depth is truncated at the target price"
        );
        let consumed = self
            .bids
            .iter()
            .take_while(|level| level.price >= target_price)
            .try_fold(0_u128, |sum, level| {
                sum.checked_add(level.size)
                    .context("depth quantity overflow")
            })?;
        anyhow::ensure!(
            consumed > 0,
            "target limit price {target_price} does not cross visible bids"
        );
        anyhow::ensure!(
            base >= consumed,
            "target raw base {base} is below visible bid quantity {consumed} at or above limit price {target_price}"
        );
        let maximum = settings.maximum_front_size.min(consumed);
        let mut available = 0_u128;
        let mut best: Option<Plan> = None;

        for level in self
            .bids
            .iter()
            .take_while(|level| level.price >= target_price)
        {
            available = available
                .checked_add(level.size)
                .context("depth quantity overflow")?;
            let front_size = aligned_size(
                available.min(maximum),
                level.price,
                self.base_scale,
                self.step,
            )?;
            if front_size < self.minimum_size {
                continue;
            }

            // The entry fills at or above this level; the fee is charged in received quote
            let front_quote = multiply_divide_floor(front_size, level.price, self.base_scale)?;
            let quote_fee_bound = multiply_divide_ceil(front_quote, self.taker_fee, FEE_SCALE)?;
            let net_proceeds = front_quote
                .checked_sub(quote_fee_bound)
                .context("entry fee exceeds proceeds")?;

            // Buying pays fees in received base, so the gross buy exceeds the sold size
            let back_size = self.buy_back_size(front_size)?;
            // After the entry and the target consume the bids, the target rests with this ask size
            let residual = base
                .checked_sub(consumed - front_size)
                .context("target residual underflow")?;
            if back_size > residual {
                continue;
            }
            let back_cost = multiply_divide_ceil(back_size, target_price, self.base_scale)?;
            if front_quote < self.minimum_notional || back_cost < self.minimum_notional {
                continue;
            }
            let costs = back_cost
                .checked_add(settings.cost_budget)
                .context("cost budget overflow")?;
            if let Some(minimum_profit) = net_proceeds.checked_sub(costs)
                && minimum_profit > 0
                && minimum_profit >= settings.minimum_profit
                && best
                    .as_ref()
                    .is_none_or(|best| minimum_profit > best.minimum_profit)
            {
                best = Some(Plan {
                    entry_is_sell: true,
                    front_size,
                    front_price: level.price,
                    front_quote_budget: front_quote,
                    back_size,
                    back_price: target_price,
                    back_quote_bound: back_cost,
                    minimum_profit,
                });
            }
            if available >= maximum {
                break;
            }
        }
        best.context("no positive conditional profit after fee and cost bounds")
    }
}

pub(super) async fn observe(
    rpc: &DeepXRpcClient,
    metadata: &Metadata,
    head: &str,
    pair: [u8; 32],
) -> anyhow::Result<Snapshot> {
    let encoded = rpc
        .get_storage(&spot_market_storage_key(pair), head)
        .await?
        .context("Spot market does not exist")?;
    let bytes = decode_hex(&encoded)?;
    let value_type = metadata
        .pallet_by_name("SpotMarket")
        .context("missing Spot pallet")?
        .storage()
        .context("missing Spot storage")?
        .entry_by_name("SpotMarkets")
        .context("missing Spot market storage")?
        .entry_type()
        .value_ty();
    let mut remaining = &bytes[..];
    let market = decode_as_type(&mut remaining, value_type, metadata.types())?;
    anyhow::ensure!(remaining.is_empty(), "trailing Spot market bytes");
    anyhow::ensure!(
        super::fixed_bytes::<32>(market.at("pair").context("missing market pair")?)? == pair,
        "market pair mismatch"
    );
    anyhow::ensure!(
        market.at("is_paused").and_then(Value::as_bool) == Some(false),
        "Spot market is paused"
    );
    anyhow::ensure!(
        field(&market, "quote_decimal")? == 6,
        "only six-decimal quote markets are supported"
    );
    let base_decimal = u32::try_from(field(&market, "base_decimal")?)?;
    let base_scale = 10_u128
        .checked_pow(base_decimal)
        .context("base precision exceeds integer range")?;
    let spec = market
        .at("order_spec")
        .context("missing Spot order specification")?;
    let minimum_size = field(spec, "min_qty")?;
    let step = field(spec, "step_size")?;
    let tick = field(spec, "tick_size")?;
    let minimum_notional = field(spec, "min_notional")?;
    let taker_fee = field(&market, "taker_fee_rate")?;
    anyhow::ensure!(
        minimum_size > 0 && step > 0 && tick > 0 && taker_fee <= FEE_SCALE,
        "invalid Spot market specification"
    );
    let encoded = rpc
        .state_call(
            "SpotRuntimeRpcApi_orderbook_depth_for_pairs",
            (vec![pair], DEPTH).encode(),
            head,
        )
        .await?;
    let raw = decode_hex(&encoded)?;
    let mut books =
        BTreeMap::<[u8; 32], (Vec<RawLevel>, Vec<RawLevel>)>::decode_all(&mut &raw[..])?;
    anyhow::ensure!(
        books.len() == 1,
        "unexpected Spot order-book response count"
    );
    let (bids, asks) = books
        .remove(&pair)
        .context("Spot order-book pair mismatch")?;
    let bids = decode_levels(bids, false)?;
    let asks = decode_levels(asks, true)?;
    anyhow::ensure!(
        bids.first()
            .zip(asks.first())
            .is_none_or(|(bid, ask)| bid.price < ask.price),
        "crossed Spot snapshot requires matching simulation"
    );
    log::debug!(
        "{}",
        json!({
            "event": "spot_book_observed",
            "observed_head": head,
            "bids": bids.len(),
            "asks": asks.len(),
            "best_bid_raw_price": bids.first().map(|level| level.price.to_string()),
            "best_ask_raw_price": asks.first().map(|level| level.price.to_string()),
            "base_scale": base_scale.to_string(),
            "taker_fee": taker_fee.to_string()
        })
    );
    Ok(Snapshot {
        base_scale,
        minimum_size,
        step,
        tick,
        minimum_notional,
        taker_fee,
        bids,
        asks,
    })
}

fn decode_levels(levels: Vec<RawLevel>, ascending: bool) -> anyhow::Result<Vec<Level>> {
    anyhow::ensure!(
        levels.len() <= DEPTH as usize,
        "oversized order-book response"
    );
    let levels = levels
        .into_iter()
        .map(|level| {
            let price = narrow(U256::from_le_bytes(level.price))?;
            let size = narrow(U256::from_le_bytes(level.size))?;
            anyhow::ensure!(
                price > 0 && size > 0 && level.order_count > 0,
                "invalid order-book level"
            );
            Ok(Level { price, size })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        levels.windows(2).all(|w| if ascending {
            w[0].price < w[1].price
        } else {
            w[0].price > w[1].price
        }),
        "unordered order-book levels"
    );
    Ok(levels)
}

fn field(value: &Value<u32>, name: &str) -> anyhow::Result<u128> {
    value
        .at(name)
        .and_then(Value::as_u128)
        .with_context(|| format!("invalid market field {name}"))
}

fn decode_hex(encoded: &str) -> anyhow::Result<Vec<u8>> {
    Ok(hex::decode(
        encoded
            .strip_prefix("0x")
            .context("missing hexadecimal prefix")?,
    )?)
}

fn narrow(value: U256) -> anyhow::Result<u128> {
    u128::try_from(value).map_err(|_| anyhow::anyhow!("financial integer exceeds u128 range"))
}

fn multiply_divide_floor(left: u128, right: u128, divisor: u128) -> anyhow::Result<u128> {
    anyhow::ensure!(divisor > 0, "zero financial divisor");
    narrow(U256::from(left) * U256::from(right) / U256::from(divisor))
}

fn aligned_size(size: u128, price: u128, scale: u128, step: u128) -> anyhow::Result<u128> {
    let lot = exact_lot(price, scale, step)?;
    Ok(size / lot * lot)
}

fn exact_lot(price: u128, scale: u128, step: u128) -> anyhow::Result<u128> {
    anyhow::ensure!(price > 0 && scale > 0 && step > 0, "invalid order units");
    let gcd = |mut a: u128, mut b: u128| {
        while b != 0 {
            (a, b) = (b, a % b);
        }
        a
    };
    let denominator = scale / gcd(price, scale);
    (step / gcd(step, denominator))
        .checked_mul(denominator)
        .context("exact order lot overflow")
}

fn multiply_divide_ceil(left: u128, right: u128, divisor: u128) -> anyhow::Result<u128> {
    anyhow::ensure!(divisor > 0, "zero financial divisor");
    let product = U256::from(left) * U256::from(right);
    let divisor = U256::from(divisor);
    let quotient = product / divisor;
    narrow(quotient + U256::from(product % divisor != U256::ZERO))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn snapshot() -> Snapshot {
        Snapshot {
            base_scale: 1_000,
            minimum_size: 10,
            step: 10,
            tick: 10_000,
            minimum_notional: 1,
            taker_fee: 0,
            bids: Vec::new(),
            asks: vec![
                Level {
                    price: 100_000_000,
                    size: 5_000,
                },
                Level {
                    price: 105_000_000,
                    size: 5_000,
                },
            ],
        }
    }

    fn target() -> serde_json::Value {
        json!({"side": "buy", "order_type": "Limit", "time_in_force": "GTC",
            "post_only": "None", "reduce_only": false,
            "raw_base_size": "10000", "raw_quote_amount": "1100000000"})
    }

    fn settings() -> Settings {
        Settings {
            maximum_front_size: 1_000,
            minimum_profit: 1,
            cost_budget: 0,
        }
    }

    #[rstest]
    fn residual_limit_bid_supports_a_conditional_round_trip() {
        let plan = snapshot().plan(&target(), &settings()).unwrap();
        assert_eq!(plan.front_size, 1_000);
        assert_eq!(plan.front_price, 100_000_000);
        assert_eq!(plan.back_size, 1_000);
        assert_eq!(plan.back_price, 110_000_000);
        assert_eq!(plan.minimum_profit, 10_000_000);
    }

    #[rstest]
    fn buy_fee_reduces_sellable_base_and_sell_fee_reduces_quote_proceeds() {
        let mut book = snapshot();
        book.taker_fee = 1_000;
        let plan = book.plan(&target(), &settings()).unwrap();
        assert_eq!(plan.back_size, 990);
        assert_eq!(plan.back_quote_bound, 108_791_100);
        assert_eq!(plan.minimum_profit, 8_791_100);
    }

    #[rstest]
    fn chooses_a_profitable_depth_prefix_instead_of_always_using_maximum_size() {
        let mut config = settings();
        config.maximum_front_size = 6_000;
        let plan = snapshot().plan(&target(), &config).unwrap();
        assert_eq!(plan.front_size, 5_000);
        assert_eq!(plan.minimum_profit, 50_000_000);
    }

    #[rstest]
    fn market_buy_does_not_create_the_bid_needed_for_a_profitable_back_leg() {
        let mut order = target();
        order["order_type"] = json!("Market");
        order["time_in_force"] = json!(null);
        assert!(snapshot().plan(&order, &settings()).is_err());
    }

    #[rstest]
    fn requires_sufficient_residual_target_liquidity() {
        let mut order = target();
        order["raw_base_size"] = json!("9000");
        order["raw_quote_amount"] = json!("990000000");
        assert!(snapshot().plan(&order, &settings()).is_err());
    }

    #[rstest]
    fn applies_cost_and_minimum_profit_budgets() {
        let mut config = settings();
        config.cost_budget = 10_000_000;
        assert!(snapshot().plan(&target(), &config).is_err());
        config.cost_budget = 0;
        config.minimum_profit = 10_000_001;
        assert!(snapshot().plan(&target(), &config).is_err());
    }

    #[rstest]
    fn refuses_incomplete_depth_or_inexact_target_terms() {
        let mut book = snapshot();
        book.asks = (0..DEPTH)
            .map(|i| Level {
                price: 100_000_000 + u128::from(i),
                size: 1,
            })
            .collect();
        assert!(
            book.plan(&target(), &settings())
                .unwrap_err()
                .to_string()
                .contains("truncated")
        );
        let mut order = target();
        order["raw_quote_amount"] = json!("1100000001");
        assert!(snapshot().plan(&order, &settings()).is_err());
    }

    #[rstest]
    fn exact_arithmetic_handles_large_intermediate_values_and_rejects_overflow() {
        assert_eq!(
            multiply_divide_floor(u128::MAX, u128::MAX, u128::MAX).unwrap(),
            u128::MAX
        );
        assert_eq!(multiply_divide_ceil(10, 10, 3).unwrap(), 34);
        assert!(multiply_divide_ceil(u128::MAX, u128::MAX, 1).is_err());
        assert!(multiply_divide_floor(1, 1, 0).is_err());
    }

    #[rstest]
    fn lot_alignment_preserves_exact_quote_mapping() {
        assert_eq!(aligned_size(999, 3, 1_000, 10).unwrap(), 0);
        assert_eq!(aligned_size(2_999, 3, 1_000, 10).unwrap(), 2_000);
        assert_eq!(aligned_size(999, 1_000, 1_000, 10).unwrap(), 990);
        assert_eq!(aligned_size(1_000, 250, 1_000, 6).unwrap(), 996);
        assert!(aligned_size(1, 0, 1_000, 1).is_err());
    }

    fn bid_snapshot() -> Snapshot {
        let mut book = snapshot();
        book.asks = Vec::new();
        book.bids = vec![
            Level {
                price: 105_000_000,
                size: 5_000,
            },
            Level {
                price: 100_000_000,
                size: 5_000,
            },
        ];
        book
    }

    fn sell_target() -> serde_json::Value {
        let mut order = target();
        order["side"] = json!("sell");
        order["raw_quote_amount"] = json!("1000000000");
        order
    }

    #[rstest]
    fn sell_target_supports_a_sell_then_buy_back_round_trip() {
        let plan = bid_snapshot().plan(&sell_target(), &settings()).unwrap();
        assert!(plan.entry_is_sell);
        assert_eq!(plan.front_size, 1_000);
        assert_eq!(plan.front_price, 105_000_000);
        assert_eq!(plan.front_quote_budget, 105_000_000);
        assert_eq!(plan.back_size, 1_000);
        assert_eq!(plan.back_price, 100_000_000);
        assert_eq!(plan.back_quote_bound, 100_000_000);
        assert_eq!(plan.minimum_profit, 5_000_000);
    }

    #[rstest]
    fn sell_fees_reduce_proceeds_and_enlarge_the_buy_back() {
        let mut book = bid_snapshot();
        book.taker_fee = 1_000;
        let mut order = sell_target();
        order["raw_base_size"] = json!("11000");
        order["raw_quote_amount"] = json!("1100000000");
        let plan = book.plan(&order, &settings()).unwrap();
        assert_eq!(plan.back_size, 1_010);
        assert_eq!(plan.back_quote_bound, 101_000_000);
        assert_eq!(plan.minimum_profit, 3_895_000);
    }

    #[rstest]
    fn sell_target_needs_residual_ask_for_the_gross_buy_back() {
        let mut book = bid_snapshot();
        book.taker_fee = 1_000;
        assert!(book.plan(&sell_target(), &settings()).is_err());
    }

    #[rstest]
    fn sell_target_above_the_best_bid_is_rejected_as_not_crossing() {
        let mut order = sell_target();
        order["raw_quote_amount"] = json!("1100000000");
        let error = bid_snapshot().plan(&order, &settings()).unwrap_err();
        assert!(error.to_string().ends_with("does not cross visible bids"));
    }

    #[rstest]
    fn sell_plan_applies_cost_and_minimum_profit_budgets() {
        let mut config = settings();
        config.cost_budget = 5_000_000;
        assert!(bid_snapshot().plan(&sell_target(), &config).is_err());
        config.cost_budget = 0;
        config.minimum_profit = 5_000_001;
        assert!(bid_snapshot().plan(&sell_target(), &config).is_err());
    }

    #[rstest]
    fn buy_back_size_covers_net_base_and_rounds_up_to_an_exact_lot() {
        let mut book = snapshot();
        assert_eq!(book.buy_back_size(1_000).unwrap(), 1_000);
        assert_eq!(book.buy_back_size(1).unwrap(), 10);
        book.taker_fee = 1_000;
        assert_eq!(book.buy_back_size(1_000).unwrap(), 1_010);
        book.taker_fee = FEE_SCALE;
        assert!(book.buy_back_size(1_000).is_err());
    }

    #[rstest]
    fn signed_entry_coverage_is_checked_for_both_directions() {
        let book = bid_snapshot();
        let plan = book.plan(&sell_target(), &settings()).unwrap();
        assert!(plan.covers_entry(105_000_000, 0, 0, 5_000_000).unwrap());
        assert!(!plan.covers_entry(105_000_000, 0, 0, 5_000_001).unwrap());
        // A signed limit above the now-supported bid would not fill completely
        assert!(!plan.covers_entry(106_000_000, 0, 0, 1).unwrap());
        let plan = snapshot().plan(&target(), &settings()).unwrap();
        assert!(plan.covers_entry(100_000_000, 0, 0, 10_000_000).unwrap());
        assert!(!plan.covers_entry(100_000_000, 0, 0, 10_000_001).unwrap());
        assert!(!plan.covers_entry(99_000_000, 0, 0, 1).unwrap());
    }

    #[rstest]
    fn malformed_or_unsorted_depth_is_rejected() {
        let level = |price: u128| RawLevel {
            price: U256::from(price).to_le_bytes(),
            size: U256::from(1).to_le_bytes(),
            order_count: 1,
        };
        assert!(decode_levels(vec![level(2), level(1)], true).is_err());
        assert!(decode_levels(vec![level(1), level(1)], true).is_err());
        assert!(decode_levels(vec![level(0)], true).is_err());
    }
}
