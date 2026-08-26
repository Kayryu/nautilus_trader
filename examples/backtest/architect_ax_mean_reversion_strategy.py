# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

from decimal import Decimal
from typing import Any, Self

from nautilus_trader.common import LogColor
from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators import BollingerBands, RelativeStrengthIndex
from nautilus_trader.model import (
    Bar,
    BarType,
    InstrumentId,
    OrderSide,
    Quantity,
    TimeInForce,
)
from nautilus_trader.trading import Strategy


class BBMeanReversionConfig(StrategyConfig):
    _CUSTOM_FIELDS = (
        "instrument_id",
        "bar_type",
        "trade_size",
        "bb_period",
        "bb_std",
        "rsi_period",
        "rsi_buy_threshold",
        "rsi_sell_threshold",
        "close_positions_on_stop",
    )

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        for key in cls._CUSTOM_FIELDS:
            kwargs.pop(key, None)
        return super().__new__(cls, *args, **kwargs)

    def __init__(
        self,
        instrument_id: InstrumentId,
        bar_type: BarType,
        trade_size: Decimal,
        bb_period: int = 20,
        bb_std: float = 2.0,
        rsi_period: int = 14,
        rsi_buy_threshold: float = 0.30,
        rsi_sell_threshold: float = 0.70,
        close_positions_on_stop: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.instrument_id = instrument_id
        self.bar_type = bar_type
        self.trade_size = trade_size
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.rsi_period = rsi_period
        self.rsi_buy_threshold = rsi_buy_threshold
        self.rsi_sell_threshold = rsi_sell_threshold
        self.close_positions_on_stop = close_positions_on_stop


class BBMeanReversion(Strategy):
    """
    Trade Bollinger Band mean reversion signals with RSI confirmation.
    """

    def __init__(self, config: BBMeanReversionConfig) -> None:
        if config.trade_size <= 0:
            raise ValueError("trade_size must be positive")

        super().__init__(config)
        self._instrument_id = config.instrument_id
        self._bar_type = config.bar_type
        self._trade_size = config.trade_size
        self._rsi_buy_threshold = config.rsi_buy_threshold
        self._rsi_sell_threshold = config.rsi_sell_threshold
        self._close_positions_on_stop = config.close_positions_on_stop
        self._instrument: Any | None = None
        self._trade_qty: Quantity | None = None
        self._bb = BollingerBands(config.bb_period, config.bb_std)
        self._rsi = RelativeStrengthIndex(config.rsi_period)

    def on_start(self) -> None:
        self._instrument = self.cache.instrument(self._instrument_id)
        if self._instrument is None:
            self.log.error(f"Could not find instrument for {self._instrument_id}")
            self.stop()
            return

        self._trade_qty = Quantity.from_decimal_dp(
            self._trade_size,
            self._instrument.size_precision,
        )

        if self._trade_qty.as_decimal() <= 0:
            self.log.error(
                f"Trade size {self._trade_size} rounds to zero for {self._instrument_id}",
            )
            self.stop()
            return

        self.register_indicator_for_bars(self._bar_type, self._bb)
        self.register_indicator_for_bars(self._bar_type, self._rsi)
        self.subscribe_bars(self._bar_type)

    def on_bar(self, bar: Bar) -> None:
        self.log.info(repr(bar), LogColor.CYAN)

        if not self.indicators_initialized():
            return
        if bar.open == bar.high == bar.low == bar.close:
            return

        close = bar.close.as_double()
        if not self._check_exit(close):
            self._check_entry(close)

    def on_stop(self) -> None:
        self.cancel_all_orders(self._instrument_id)
        if self._close_positions_on_stop:
            self.close_all_positions(self._instrument_id)
        self.unsubscribe_bars(self._bar_type)

    def on_reset(self) -> None:
        self._instrument = None
        self._trade_qty = None
        self._bb.reset()
        self._rsi.reset()

    def _check_exit(self, close: float) -> bool:
        if self.portfolio.is_net_long(self._instrument_id) and close >= self._bb.middle:
            self.close_all_positions(self._instrument_id)
            return True
        if self.portfolio.is_net_short(self._instrument_id) and close <= self._bb.middle:
            self.close_all_positions(self._instrument_id)
            return True
        return False

    def _check_entry(self, close: float) -> None:
        if close <= self._bb.lower and self._rsi.value < self._rsi_buy_threshold:
            if self.portfolio.is_net_short(self._instrument_id):
                self.close_all_positions(self._instrument_id)
            if not self.portfolio.is_net_long(self._instrument_id):
                self._submit_market_order(OrderSide.BUY)
        elif close >= self._bb.upper and self._rsi.value > self._rsi_sell_threshold:
            if self.portfolio.is_net_long(self._instrument_id):
                self.close_all_positions(self._instrument_id)
            if not self.portfolio.is_net_short(self._instrument_id):
                self._submit_market_order(OrderSide.SELL)

    def _submit_market_order(self, order_side: OrderSide) -> None:
        if self._trade_qty is None:
            return

        order = self.order_factory.market(
            instrument_id=self._instrument_id,
            order_side=order_side,
            quantity=self._trade_qty,
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)
