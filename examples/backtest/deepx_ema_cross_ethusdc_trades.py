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
"""Backtest an EMA cross on public DeepX testnet ETH-USDC perpetual trades."""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from nautilus_trader.adapters.deepx import DEEPX_VENUE, load_deepx_perp_trade_ticks
from nautilus_trader.backtest import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.model import (
    AccountType,
    BarType,
    Currency,
    Money,
    OmsType,
    TraderId,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "docs" / "tutorials"))

from ema_cross import EMACross, EMACrossConfig

MARKET_ID = 3
TRADE_LIMIT = 1_000
TRADES_PER_BAR = 10


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-ms", type=int)
    parser.add_argument("--end-ms", type=int)
    args = parser.parse_args()
    if (args.start_ms is None) != (args.end_ms is None):
        parser.error("--start-ms and --end-ms must be supplied together")
    end = datetime.now(timezone.utc)
    end_ms = args.end_ms if args.end_ms is not None else int(end.timestamp() * 1_000)
    start_ms = (
        args.start_ms
        if args.start_ms is not None
        else int((end - timedelta(days=1)).timestamp() * 1_000)
    )
    if start_ms >= end_ms:
        parser.error("--start-ms must precede --end-ms")

    instrument, ticks = load_deepx_perp_trade_ticks(
        market_id=MARKET_ID,
        start_ms=start_ms,
        end_ms=end_ms,
        limit=TRADE_LIMIT,
        max_pages=20,
    )
    if len(ticks) < TRADES_PER_BAR * 30:
        raise RuntimeError("DeepX returned fewer than 300 trades for the EMA warmup")

    engine = BacktestEngine(
        BacktestEngineConfig(trader_id=TraderId.from_str("BACKTESTER-001"))
    )
    try:
        usdc = Currency.from_str("USDC")
        engine.add_venue(
            venue=DEEPX_VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            base_currency=usdc,
            starting_balances=[Money.from_str("10000 USDC")],
        )
        engine.add_instrument(instrument)
        engine.add_data(ticks)
        bar_type = BarType.from_str(
            f"{instrument.id}-{TRADES_PER_BAR}-TICK-LAST-INTERNAL"
        )
        engine.add_strategy(
            EMACross(
                EMACrossConfig(
                    instrument_id=instrument.id,
                    bar_type=bar_type,
                    trade_size=Decimal("0.01"),
                    fast_ema_period=5,
                    slow_ema_period=20,
                ),
            ),
        )
        engine.run()
        print(engine.generate_account_report(DEEPX_VENUE))
        print(engine.generate_order_fills_report())
        print(engine.generate_positions_report())
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
