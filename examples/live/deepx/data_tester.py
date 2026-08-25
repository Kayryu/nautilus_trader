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
Stream DeepX market data with the built-in DataTester actor.

"""

from __future__ import annotations

from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import TraderId


DEEPX = "DEEPX"
TRADER_ID = TraderId.from_str("TESTER-001")
INSTRUMENT_ID = InstrumentId.from_str(f"ETH-USDC-PERP.{DEEPX}")


def main() -> None:
    _ = LiveNode.builder("DEEPX-DATA-TESTER-001", TRADER_ID, Environment.LIVE)

    print("DeepX data tester placeholder")
    print("DeepX live data client bindings are not yet available in Python API")
    print(f"Planned instrument: {INSTRUMENT_ID}")


if __name__ == "__main__":
    main()
