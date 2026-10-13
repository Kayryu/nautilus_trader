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
"""Stream public DeepX testnet market data without credentials or orders."""

import argparse

from nautilus_trader.adapters.deepx import (
    DEEPX,
    DeepXDataClientConfig,
    DeepXDataClientFactory,
)
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import ClientId, InstrumentId, TraderId
from nautilus_trader.testkit import DataTesterConfig

PERPETUAL_INSTRUMENT_ID = InstrumentId.from_str("ETH-USDC-PERP.DEEPX")
SPOT_INSTRUMENT_ID = InstrumentId.from_str("ETH-USDC.DEEPX")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spot", action="store_true", help="stream ETH/USDC Spot data")
    args = parser.parse_args()

    node = (
        LiveNode.builder(
            "DEEPX-DATA-TESTER-001",
            TraderId.from_str("TESTER-001"),
            Environment.LIVE,
        )
        .add_data_client(
            None,
            DeepXDataClientFactory(),
            DeepXDataClientConfig(perpetual_only=not args.spot),
        )
        .build()
    )
    node.add_builtin_actor(
        "DataTester",
        DataTesterConfig(
            client_id=ClientId.from_str(DEEPX),
            instrument_ids=[
                SPOT_INSTRUMENT_ID if args.spot else PERPETUAL_INSTRUMENT_ID
            ],
            subscribe_quotes=True,
            subscribe_trades=True,
            subscribe_mark_prices=not args.spot,
            subscribe_index_prices=not args.spot,
            subscribe_funding_rates=not args.spot,
            subscribe_book_at_interval=True,
            book_interval_ms=1000,
            manage_book=True,
            log_data=True,
        ),
    )
    node.run()


if __name__ == "__main__":
    main()
