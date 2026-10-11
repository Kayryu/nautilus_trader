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

"""Tests for read-only grouped Spot wallet trades."""

import json
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from types import SimpleNamespace
from urllib.parse import parse_qs
from urllib.parse import urlparse

import pytest

from nautilus_trader.adapters.deepx import DeepXDataClientConfig
from nautilus_trader.adapters.deepx import DeepXNetworkConfig
from nautilus_trader.adapters.deepx import DeepXSpotAccountTradeRecord
from nautilus_trader.adapters.deepx import DeepXSpotWalletTradesPage
from nautilus_trader.adapters.deepx import get_deepx_spot_wallet_trade_pages
from nautilus_trader.adapters.deepx import get_deepx_spot_wallet_trades


CAPTURED_WALLET = "0x89bb0946046588f4f257ffc71bd3231aba13474a"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/test_data/http/testnet/spot_wallet_trades_eth_usdc_page_1.json"
)


@pytest.fixture
def spot_trade_api() -> Iterator[SimpleNamespace]:
    """Serve the captured public Spot wallet trade page from loopback."""
    state = SimpleNamespace(responses=[json.loads(FIXTURE.read_bytes())], paths=[])

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.paths.append(self.path)
            body = json.dumps(state.responses[len(state.paths) - 1]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.config = DeepXDataClientConfig(
        network=DeepXNetworkConfig(base_url_rest=f"http://127.0.0.1:{server.server_port}"),
    )
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_spot_wallet_trades_preserve_exact_values(spot_trade_api: SimpleNamespace) -> None:
    """Expose the validated group and original decimal values."""
    page = get_deepx_spot_wallet_trades(
        CAPTURED_WALLET,
        name="ETH/USDC",
        page_size=5,
        config=spot_trade_api.config,
    )

    assert isinstance(page, DeepXSpotWalletTradesPage)
    assert page.has_next
    assert len(page.markets) == 1
    market = page.markets[0]
    assert market.name == "ETH/USDC"
    assert market.pair.startswith("0x9068d4ac")
    assert len(market.subaccounts) == 1
    group = market.subaccounts[0]
    assert group.subaccount == "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c"
    assert group.trades.next_cursor == page.next_cursor
    assert len(group.trades.items) == 5
    trade = group.trades.items[0]
    assert isinstance(trade, DeepXSpotAccountTradeRecord)
    assert trade.id == 188632828000015
    assert trade.order_id == "1789631491887"
    assert trade.order_side == "Sell"
    assert trade.base_amount == "0.26400108068721884"
    assert trade.quote_amount == "644.9256"
    assert trade.price == "2442.89"
    assert trade.fee == "-0.064492"
    assert trade.fee_asset == ""
    assert trade.taker == "Buyer"
    assert urlparse(spot_trade_api.paths[0]).path == "/internal/v1/account/spot/trades-by-wallet"
    assert parse_qs(urlparse(spot_trade_api.paths[0]).query) == {
        "address": [CAPTURED_WALLET],
        "name": ["ETH/USDC"],
        "sort": ["DESC"],
        "pageSize": ["5"],
    }


def test_spot_wallet_trade_pages_follow_global_cursor(spot_trade_api: SimpleNamespace) -> None:
    """Continue with the one cursor shared by the groups."""
    first = spot_trade_api.responses[0]
    second = json.loads(json.dumps(first))
    second["data"][0]["subaccounts"][0]["trades"] = {
        "items": [],
        "nextCursor": None,
        "hasNext": False,
    }
    spot_trade_api.responses = [first, second]

    pages = get_deepx_spot_wallet_trade_pages(
        CAPTURED_WALLET,
        2,
        name="ETH/USDC",
        page_size=5,
        config=spot_trade_api.config,
    )

    assert len(pages) == 2
    assert len(pages[0].markets[0].subaccounts[0].trades.items) == 5
    assert not pages[1].has_next
    assert parse_qs(urlparse(spot_trade_api.paths[1]).query)["cursor"] == [
        pages[0].next_cursor,
    ]


def test_spot_wallet_trade_pages_reject_duplicate_ids(spot_trade_api: SimpleNamespace) -> None:
    """Reject repeated trade identities across mutable pages."""
    first = spot_trade_api.responses[0]
    second = json.loads(json.dumps(first))
    trades = second["data"][0]["subaccounts"][0]["trades"]
    trades["items"] = trades["items"][-1:]
    trades["nextCursor"] = None
    trades["hasNext"] = False
    spot_trade_api.responses = [first, second]

    with pytest.raises(ValueError, match="repeats trade"):
        get_deepx_spot_wallet_trade_pages(
            CAPTURED_WALLET,
            2,
            name="ETH/USDC",
            page_size=5,
            config=spot_trade_api.config,
        )


def test_spot_wallet_trade_request_rejected_before_network(spot_trade_api: SimpleNamespace) -> None:
    """Reject an absent market selector before contacting the venue."""
    with pytest.raises(ValueError, match="exactly one nonempty name or bytes32 pair"):
        get_deepx_spot_wallet_trades(CAPTURED_WALLET, config=spot_trade_api.config)

    assert spot_trade_api.paths == []
