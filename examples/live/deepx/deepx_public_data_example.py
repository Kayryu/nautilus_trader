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
DeepX live public-data example.

Run:
    python examples/live/deepx/deepx_public_data_example.py
"""

from __future__ import annotations

import json

import requests

from nautilus_trader.adapters.deepx import DEEPX
from nautilus_trader.adapters.deepx import DeepXOrderBookSnapshot
from nautilus_trader.adapters.deepx import DeepXOrderBookUpdate
from nautilus_trader.adapters.deepx import DeepXPerpetualMarket
from nautilus_trader.adapters.deepx import DeepXTrade


DEEPX_TESTNET_REST_URL = "https://rest-api-testnet.deepx.fi"
REST_TIMEOUT_SECS = 10


def fetch_perpetual_markets() -> list[DeepXPerpetualMarket]:
    response = requests.get(
        f"{DEEPX_TESTNET_REST_URL}/v1/perp/markets",
        timeout=REST_TIMEOUT_SECS,
    )
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(f"Expected list payload from DeepX markets endpoint, was {type(payload)}")

    return [
        DeepXPerpetualMarket.from_json(json.dumps(item, separators=(",", ":")))
        for item in payload
    ]


def parse_public_samples() -> tuple[DeepXOrderBookSnapshot, DeepXOrderBookUpdate, DeepXTrade]:
    book_snapshot = DeepXOrderBookSnapshot.from_json(
        """{
            "asks":[["2453.76","3.282"]],"bids":[["2453.75","0.842"]],
            "engineTime":1787561622066,"lastUpdateId":62430652,
            "serverTime":1787561622211
        }""",
    )
    ws_snapshot = DeepXOrderBookUpdate.from_json(
        """{"channel":"perp@orderbook","data":{
            "asks":[["2457.02","0.859"]],"bids":[["2456.81","0.291"]],
            "engineTime":1787562160783,"lastUpdateId":62493922,
            "prevLastUpdateId":null,"serverTime":1787562160833,
            "symbol":"ETH-USDC","updateType":"snapshot"
        }}""",
    )
    trade = DeepXTrade.from_json(
        """{"channel":"perp@trades","data":{
            "buyOrderId":"6652970","id":"159078803000024","makerFee":"-0.02529",
            "marketId":3,"price":"2455.7","qty":"0.103","quoteQty":"252.9371",
            "sellOrderId":"2263022","symbol":"ETH-USDC","takerFee":"0.050581",
            "takerSide":"SELL","time":1787562119833
        }}""",
    )
    return book_snapshot, ws_snapshot, trade


def main() -> None:
    markets = fetch_perpetual_markets()
    book_snapshot, ws_snapshot, trade = parse_public_samples()

    print(f"Venue: {DEEPX}")
    print(f"Markets fetched: {len(markets)}")
    if markets:
        first = markets[0]
        print(
            "First market: "
            f"symbol={first.symbol} tick_size={first.tick_size} step_size={first.step_size}"
        )

    print(
        "Order book snapshot: "
        f"last_update_id={book_snapshot.last_update_id} best_bid={book_snapshot.bids[0]}"
    )
    print(
        "WS snapshot: "
        f"symbol={ws_snapshot.symbol} last_update_id={ws_snapshot.last_update_id}"
    )
    print(f"Trade: id={trade.id} side={trade.taker_side} price={trade.price} qty={trade.qty}")


if __name__ == "__main__":
    main()
