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
"""Run a simple dual moving-average strategy on the DeepX testnet."""

import argparse
import os
from collections.abc import Sequence
from decimal import Decimal
from typing import Any, Self

from nautilus_trader.adapters.deepx import (
    DeepXDataClientConfig,
    DeepXDataClientFactory,
    DeepXExecutionClientConfig,
    DeepXExecutionClientFactory,
    get_deepx_wallet_account_snapshot,
)
from nautilus_trader.common import Environment, LogColor
from nautilus_trader.config import LiveExecutionEngineConfig, StrategyConfig
from nautilus_trader.core import NANOSECONDS_IN_SECOND, unix_nanos_to_dt
from nautilus_trader.indicators import SimpleMovingAverage
from nautilus_trader.infrastructure import PostgresCacheConfig, PostgresConnectOptions
from nautilus_trader.live import LiveNode
from nautilus_trader.model import (
    AccountId,
    Bar,
    BarType,
    InstrumentId,
    OrderSide,
    PositionClosed,
    Quantity,
    StrategyId,
    TimeInForce,
    TraderId,
)
from nautilus_trader.trading import Strategy

WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
INSTRUMENT_ID = InstrumentId.from_str("ETH-USDC-PERP.DEEPX")
BAR_TYPE = BarType.from_str(f"{INSTRUMENT_ID}-1-MINUTE-LAST-EXTERNAL")
ORDER_QTY = Decimal("0.500")
FAST_PERIOD = 5
SLOW_PERIOD = 20
DEFAULT_TIMESTAMP_NONCE_MAX_CLOCK_DRIFT_MS = 10_000
NANOSECONDS_IN_MINUTE = 60 * NANOSECONDS_IN_SECOND
WARMUP_LOOKBACK_MINUTES = 60
WARMUP_RETRY_DELAY_SECONDS = 2
WARMUP_MAX_ATTEMPTS = 3
WARMUP_RETRY_TIMER = "deepx-warmup-retry"


class DualMovingAverageConfig(StrategyConfig):
    """Configuration for the dual moving-average strategy."""

    _CUSTOM_FIELDS = (
        "instrument_id",
        "bar_type",
        "order_qty",
        "fast_period",
        "slow_period",
        "trade_enabled",
    )

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        for key in cls._CUSTOM_FIELDS:
            kwargs.pop(key, None)
        return super().__new__(cls, *args, **kwargs)

    def __init__(
        self,
        instrument_id: InstrumentId,
        bar_type: BarType,
        order_qty: Decimal,
        fast_period: int,
        slow_period: int,
        trade_enabled: bool,
        **_kwargs: Any,
    ) -> None:
        super().__init__()
        self.instrument_id = instrument_id
        self.bar_type = bar_type
        self.order_qty = order_qty
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.trade_enabled = trade_enabled


class DualMovingAverage(Strategy):
    """Trade when the fast simple moving average crosses the slow average."""

    def __init__(self, config: DualMovingAverageConfig) -> None:
        if config.fast_period <= 0:
            raise ValueError("fast_period must be positive")
        if config.slow_period <= config.fast_period:
            raise ValueError("slow_period must be greater than fast_period")
        if config.order_qty <= 0:
            raise ValueError("order_qty must be positive")

        super().__init__(config)
        self._config = config
        self._fast = SimpleMovingAverage(config.fast_period)
        self._slow = SimpleMovingAverage(config.slow_period)
        self._quantity: Quantity | None = None
        self._previous_fast_above: bool | None = None
        self._pending_side: OrderSide | None = None
        self._warmup_complete = False
        self._subscribed = False
        self._warmup_attempts = 0

    def on_start(self) -> None:
        instrument = self.cache.instrument(self._config.instrument_id)
        if instrument is None:
            self.log.error(
                f"Could not find instrument for {self._config.instrument_id}"
            )
            self.stop()
            return

        self._quantity = Quantity.from_decimal_dp(
            self._config.order_qty,
            instrument.size_precision,
        )
        if self._quantity.as_decimal() <= 0:
            self.log.error(
                f"Order quantity {self._config.order_qty} rounds to zero for "
                f"{self._config.instrument_id}",
            )
            self.stop()
            return

        self.register_indicator_for_bars(self._config.bar_type, self._fast)
        self.register_indicator_for_bars(self._config.bar_type, self._slow)
        mode = "trading" if self._config.trade_enabled else "signal-only"
        self.log.info(
            f"Starting dual moving-average strategy in {mode} mode; "
            f"requesting up to {WARMUP_LOOKBACK_MINUTES} historical bars",
            LogColor.BLUE,
        )
        self._request_warmup_bars()

    def _request_warmup_bars(self) -> None:
        self._warmup_attempts += 1
        current_minute_start = (
            self.clock.timestamp_ns() // NANOSECONDS_IN_MINUTE
        ) * NANOSECONDS_IN_MINUTE
        history_start = (
            current_minute_start - WARMUP_LOOKBACK_MINUTES * NANOSECONDS_IN_MINUTE
        )
        self.request_bars(
            self._config.bar_type,
            start=unix_nanos_to_dt(history_start),
            end=unix_nanos_to_dt(current_minute_start - 1),
            limit=WARMUP_LOOKBACK_MINUTES,
        )

    def on_bar(self, bar: Bar) -> None:
        if not self._warmup_complete:
            return

        fast_above = self._fast.value > self._slow.value
        self.log.info(
            f"{bar.bar_type} close={bar.close} fast={self._fast.value:.6f} "
            f"slow={self._slow.value:.6f}",
            LogColor.CYAN,
        )
        if self._previous_fast_above is None:
            self._previous_fast_above = fast_above
            return
        if fast_above == self._previous_fast_above:
            return

        self._previous_fast_above = fast_above
        side = OrderSide.BUY if fast_above else OrderSide.SELL
        self.log.info(f"Moving-average crossover signal: {side}", LogColor.BLUE)
        if self._config.trade_enabled:
            self._handle_signal(side)

    def on_historical_bars(self, bars: Sequence[Bar]) -> None:
        if self._warmup_complete:
            return
        if len(bars) == 0 and self._warmup_attempts < WARMUP_MAX_ATTEMPTS:
            self.log.warning(
                f"Historical warmup returned no bars on attempt "
                f"{self._warmup_attempts}/{WARMUP_MAX_ATTEMPTS}; retrying in "
                f"{WARMUP_RETRY_DELAY_SECONDS} seconds",
            )
            self.clock.set_time_alert_ns(
                WARMUP_RETRY_TIMER,
                self.clock.timestamp_ns()
                + WARMUP_RETRY_DELAY_SECONDS * NANOSECONDS_IN_SECOND,
                lambda _event: self._request_warmup_bars(),
            )
            return
        if len(bars) < self._config.slow_period or not self.indicators_initialized():
            self.log.error(
                f"Historical warmup requires {self._config.slow_period} bars, "
                f"received {len(bars)}",
            )
            return

        self._previous_fast_above = self._fast.value > self._slow.value
        self._warmup_complete = True
        self.subscribe_bars(self._config.bar_type)
        self._subscribed = True
        self.log.info(
            f"Historical warmup complete with {len(bars)} bars; subscribed to live bars",
            LogColor.BLUE,
        )

    def on_position_closed(self, event: PositionClosed) -> None:
        if (
            event.instrument_id != self._config.instrument_id
            or self._pending_side is None
        ):
            return
        side = self._pending_side
        self._pending_side = None
        self.log.info(
            f"Position closed; submitting pending {side} order",
            LogColor.BLUE,
        )
        self._submit_market_order(side)

    def on_stop(self) -> None:
        if self._config.trade_enabled:
            self.close_all_positions(
                self._config.instrument_id,
                time_in_force=TimeInForce.IOC,
                reduce_only=True,
            )
        if self._subscribed:
            self.unsubscribe_bars(self._config.bar_type)

    def on_reset(self) -> None:
        self._quantity = None
        self._previous_fast_above = None
        self._pending_side = None
        self._warmup_complete = False
        self._subscribed = False
        self._warmup_attempts = 0
        self._fast.reset()
        self._slow.reset()

    def _handle_signal(self, side: OrderSide) -> None:
        if self._pending_side is not None:
            self.log.info(
                f"Deferring {side} signal while position close is pending; "
                f"replacing pending side {self._pending_side}",
                LogColor.BLUE,
            )
            self._pending_side = side
            return
        if side == OrderSide.BUY and self.portfolio.is_net_long(
            self._config.instrument_id
        ):
            self.log.info(
                "Skipping BUY signal because the portfolio is already net long",
                LogColor.BLUE,
            )
            return
        if side == OrderSide.SELL and self.portfolio.is_net_short(
            self._config.instrument_id
        ):
            self.log.info(
                "Skipping SELL signal because the portfolio is already net short",
                LogColor.BLUE,
            )
            return
        if not self.portfolio.is_net_flat(self._config.instrument_id):
            self.log.info(
                f"Closing the existing position before submitting {side}",
                LogColor.BLUE,
            )
            self._pending_side = side
            self.close_all_positions(
                self._config.instrument_id,
                time_in_force=TimeInForce.IOC,
                reduce_only=True,
            )
            return
        self._submit_market_order(side)

    def _submit_market_order(self, side: OrderSide) -> None:
        if self._quantity is None:
            return
        order = self.order_factory.market(
            instrument_id=self._config.instrument_id,
            order_side=side,
            quantity=self._quantity,
            time_in_force=TimeInForce.IOC,
        )
        self.log.info(
            f"Submitting {side} market order for {self._quantity}",
            LogColor.BLUE,
        )
        self.submit_order(order)


def select_subaccount(requested: str | None, addresses: list[str]) -> str:
    """Select a wallet-owned DeepX subaccount."""
    if not addresses:
        raise ValueError("the specified wallet currently has no DeepX subaccounts")
    if requested is not None:
        for address in addresses:
            if address.lower() == requested.lower():
                return address
        raise ValueError("subaccount is absent from the specified wallet directory")
    if len(addresses) != 1:
        raise ValueError(
            "specify --subaccount when the wallet has multiple subaccounts"
        )
    return addresses[0]


def positive_int(value: str) -> int:
    """Parse a positive integer command-line argument."""
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def postgres_options() -> tuple[PostgresConnectOptions, PostgresCacheConfig]:
    """Read durable transaction store settings from the environment."""
    required = ("POSTGRES_USERNAME", "POSTGRES_PASSWORD", "POSTGRES_DATABASE")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError(f"missing PostgreSQL settings: {', '.join(missing)}")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    username = os.environ["POSTGRES_USERNAME"]
    password = os.environ["POSTGRES_PASSWORD"]
    database = os.environ["POSTGRES_DATABASE"]
    return (
        PostgresConnectOptions(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database,
        ),
        PostgresCacheConfig(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--subaccount", help="AccountId20 from the specified wallet directory"
    )
    parser.add_argument(
        "--trade",
        action="store_true",
        help="enable testnet order submission; otherwise only log crossover signals",
    )
    parser.add_argument(
        "--timestamp-nonce-max-clock-drift-ms",
        type=positive_int,
        default=DEFAULT_TIMESTAMP_NONCE_MAX_CLOCK_DRIFT_MS,
        help="maximum local-to-chain clock drift for testnet order nonces",
    )
    args = parser.parse_args()

    builder = (
        LiveNode.builder(
            "DEEPX-DUAL-MA-001",
            TraderId.from_str("TESTER-001"),
            Environment.LIVE,
        )
        .with_shutdown_on_error(True)
        .add_data_client(
            None,
            DeepXDataClientFactory(),
            DeepXDataClientConfig(perpetual_only=True),
        )
    )

    if args.trade:
        if not os.environ.get("DEEPX_TESTNET_PRIVATE_KEY"):
            parser.error(
                "DEEPX_TESTNET_PRIVATE_KEY is required when --trade is enabled"
            )
        snapshot = get_deepx_wallet_account_snapshot(WALLET)
        subaccount = select_subaccount(args.subaccount, snapshot.directory.addresses)
        database, cache_database = postgres_options()
        builder = (
            builder.with_cache_database_factory(cache_database)
            .with_timeout_connection(180)
            .with_exec_engine_config(
                LiveExecutionEngineConfig(
                    reconciliation=True,
                    open_check_interval_secs=10.0,
                    open_check_open_only=True,
                ),
            )
            .add_exec_client(
                None,
                DeepXExecutionClientFactory(),
                DeepXExecutionClientConfig(
                    account_id=AccountId.from_str("DEEPX-001"),
                    subaccount_id=subaccount,
                    timestamp_nonce_max_clock_drift_ms=(
                        args.timestamp_nonce_max_clock_drift_ms
                    ),
                    postgres_cache_database_config=database,
                ),
            )
        )

    node = builder.build()
    node.add_strategy(
        DualMovingAverage(
            DualMovingAverageConfig(
                strategy_id=StrategyId.from_str("DEEPX-DUAL-MA-001"),
                external_order_claims=[INSTRUMENT_ID],
                instrument_id=INSTRUMENT_ID,
                bar_type=BAR_TYPE,
                order_qty=ORDER_QTY,
                fast_period=FAST_PERIOD,
                slow_period=SLOW_PERIOD,
                trade_enabled=args.trade,
            ),
        ),
    )
    node.run()


if __name__ == "__main__":
    main()
