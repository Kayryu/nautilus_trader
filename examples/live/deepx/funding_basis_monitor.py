#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""Observe DeepX Spot/Perpetual basis and funding without submitting orders."""

import argparse
from decimal import Decimal
from typing import Any, Self

from nautilus_trader.adapters.deepx import DeepXDataClientConfig, DeepXDataClientFactory
from nautilus_trader.common import Environment
from nautilus_trader.config import StrategyConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.model import (
    FundingRateUpdate,
    InstrumentId,
    MarkPriceUpdate,
    QuoteTick,
    StrategyId,
    TraderId,
)
from nautilus_trader.trading import Strategy

SPOT_ID = InstrumentId.from_str("ETH-USDC.DEEPX")
PERP_ID = InstrumentId.from_str("ETH-USDC-PERP.DEEPX")
NANOSECONDS_PER_SECOND = 1_000_000_000
REPORT_INTERVAL_NS = 5 * NANOSECONDS_PER_SECOND


def positive_decimal(value: str) -> Decimal:
    parsed = Decimal(value)
    if not parsed.is_finite() or parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a finite positive decimal")
    return parsed


def nonnegative_decimal(value: str) -> Decimal:
    parsed = Decimal(value)
    if not parsed.is_finite() or parsed < 0:
        raise argparse.ArgumentTypeError("value must be a finite nonnegative decimal")
    return parsed


class FundingBasisMonitorConfig(StrategyConfig):
    _CUSTOM_FIELDS = ("quantity", "round_trip_cost_bps", "max_age_seconds")

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        for key in cls._CUSTOM_FIELDS:
            kwargs.pop(key, None)
        return super().__new__(cls, *args, **kwargs)

    def __init__(
        self,
        quantity: Decimal,
        round_trip_cost_bps: Decimal,
        max_age_seconds: int,
        **_kwargs: Any,
    ) -> None:
        super().__init__()
        self.quantity = quantity
        self.round_trip_cost_bps = round_trip_cost_bps
        self.max_age_ns = max_age_seconds * NANOSECONDS_PER_SECOND


class FundingBasisMonitor(Strategy):
    def __init__(self, config: FundingBasisMonitorConfig) -> None:
        super().__init__(config)
        self._config = config
        self._quotes: dict[InstrumentId, QuoteTick] = {}
        self._funding: FundingRateUpdate | None = None
        self._mark: MarkPriceUpdate | None = None
        self._last_report_ns: int | None = None

    def on_start(self) -> None:
        for instrument_id in (SPOT_ID, PERP_ID):
            if self.cache.instrument(instrument_id) is None:
                self.log.error(f"Missing DeepX instrument {instrument_id}")
                self.stop()
                return
            self.subscribe_quotes(instrument_id)
        self.subscribe_funding_rates(PERP_ID)
        self.subscribe_mark_prices(PERP_ID)

    def on_quote(self, quote: QuoteTick) -> None:
        if quote.instrument_id not in (SPOT_ID, PERP_ID):
            return
        self._quotes[quote.instrument_id] = quote
        self._report()

    def on_funding_rate(self, funding_rate: FundingRateUpdate) -> None:
        if funding_rate.instrument_id != PERP_ID:
            return
        self._funding = funding_rate
        self._report()

    def on_mark_price(self, mark_price: MarkPriceUpdate) -> None:
        if mark_price.instrument_id == PERP_ID:
            self._mark = mark_price

    def _report(self) -> None:
        spot = self._quotes.get(SPOT_ID)
        perp = self._quotes.get(PERP_ID)
        funding = self._funding
        if spot is None or perp is None or funding is None:
            return
        now = self.clock.timestamp_ns()
        if (
            self._last_report_ns is not None
            and now - self._last_report_ns < REPORT_INTERVAL_NS
        ):
            return
        if any(
            timestamp > now or now - timestamp > self._config.max_age_ns
            for timestamp in (spot.ts_event, perp.ts_event, funding.ts_event)
        ):
            return
        if (
            spot.ask_size.as_decimal() < self._config.quantity
            or perp.bid_size.as_decimal() < self._config.quantity
        ):
            return
        spot_ask = spot.ask_price.as_decimal()
        perp_bid = perp.bid_price.as_decimal()
        if spot_ask <= 0 or perp_bid <= 0:
            return
        quantity = self._config.quantity
        entry_basis = perp_bid - spot_ask
        entry_cashflow = quantity * entry_basis
        round_trip_cost = (
            quantity
            * (spot_ask + perp_bid)
            * self._config.round_trip_cost_bps
            / Decimal(10_000)
        )
        mark = self._mark
        mark_value = (
            str(mark.value)
            if mark is not None and 0 <= now - mark.ts_event <= self._config.max_age_ns
            else "unavailable"
        )
        self.log.info(
            f"spot_ask={spot_ask} perp_bid={perp_bid} entry_basis={entry_basis} "
            f"entry_cashflow={entry_cashflow} estimated_round_trip_cost={round_trip_cost} "
            f"funding_sample={funding.rate} mark={mark_value} "
            f"funding_interval={funding.interval} next_funding_ns={funding.next_funding_ns}"
        )
        self._last_report_ns = now


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quantity", type=positive_decimal, default=Decimal("0.5"))
    parser.add_argument(
        "--round-trip-cost-bps",
        type=nonnegative_decimal,
        default=Decimal(0),
        help="round-trip cost budget applied to the sum of entry notionals",
    )
    parser.add_argument("--max-age-seconds", type=int, default=10)
    args = parser.parse_args()
    if args.max_age_seconds <= 0:
        parser.error("--max-age-seconds must be positive")

    node = (
        LiveNode.builder(
            "DEEPX-FUNDING-BASIS-001",
            TraderId.from_str("TESTER-001"),
            Environment.LIVE,
        )
        .with_shutdown_on_error(True)
        .add_data_client(
            None,
            DeepXDataClientFactory(),
            DeepXDataClientConfig(perpetual_only=False),
        )
        .build()
    )
    node.add_strategy(
        FundingBasisMonitor(
            FundingBasisMonitorConfig(
                strategy_id=StrategyId.from_str("DEEPX-FUNDING-BASIS-001"),
                quantity=args.quantity,
                round_trip_cost_bps=args.round_trip_cost_bps,
                max_age_seconds=args.max_age_seconds,
            ),
        ),
    )
    node.run()


if __name__ == "__main__":
    main()
