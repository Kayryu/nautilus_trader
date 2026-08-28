#!/usr/bin/env python3
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
"""
Request DeepX instruments and an order book snapshot with the built-in DataTester actor.

Running this example connects to DeepX testnet and immediately requests the perpetual market
catalog and one L2 order book snapshot. No orders are placed.

"""

from __future__ import annotations

from nautilus_trader.adapters.deepx import (
    DEEPX,
    DeepXDataClientConfig,
    DeepXDataClientFactory,
    DeepXEnvironment,
)
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import ClientId, InstrumentId, TraderId
from nautilus_trader.testkit import DataTesterConfig

TRADER_ID = TraderId.from_str("TESTER-001")
INSTRUMENT_ID = InstrumentId.from_str(f"ETH-USDC-PERP.{DEEPX}")


def main() -> None:
    """
    Run the example.
    """
    node = (
        LiveNode.builder("DEEPX-DATA-TESTER-001", TRADER_ID, Environment.LIVE)
        .add_data_client(
            DEEPX,
            DeepXDataClientFactory(),
            DeepXDataClientConfig(environment=DeepXEnvironment.TESTNET),
        )
        .build()
    )
    node.add_builtin_actor(
        "DataTester",
        DataTesterConfig(
            client_id=ClientId.from_str(DEEPX),
            instrument_ids=[INSTRUMENT_ID],
            request_instruments=True,
            request_book_snapshot=True,
            manage_book=True,
            log_data=True,
            stats_interval_secs=0,
        ),
    )

    node.run()


if __name__ == "__main__":
    main()
