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

"""Tests for read-only Spot account trade Python bindings."""

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
from nautilus_trader.adapters.deepx import DeepXSpotAccountTradesPage
from nautilus_trader.adapters.deepx import get_deepx_spot_account_trade_pages
from nautilus_trader.adapters.deepx import get_deepx_spot_account_trades


SUBACCOUNT = "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c"
ORDER_ID = "1789627349907"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/test_data/http/testnet/spot_account_trades_eth_usdc_page_1.json"
)


@pytest.fixture
def account_trade_api() -> Iterator[SimpleNamespace]:
    """Serve the captured account trade page and an empty continuation."""
    first = json.loads(FIXTURE.read_bytes())
    second = {
        "code": 200,
        "msg": "success",
        "data": {"items": [], "hasNext": False, "nextCursor": None},
        "fail": False,
    }
    state = SimpleNamespace(responses=[first, second], paths=[])

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


def test_spot_account_trades_preserve_signed_fee(account_trade_api: SimpleNamespace) -> None:
    """Expose exact trade values and the documented order filter."""
    page = get_deepx_spot_account_trades(
        SUBACCOUNT,
        order_id=ORDER_ID,
        order_side="Sell",
        name="ETH/USDC",
        page_size=3,
        config=account_trade_api.config,
    )

    assert isinstance(page, DeepXSpotAccountTradesPage)
    assert page.has_next
    assert len(page.items) == 3
    assert page.items[0].id == 188573374000099
    assert page.items[0].base_amount == "0.33079181307615474"
    assert page.items[0].fee == "-0.323216"
    assert page.items[0].fee_asset == ""
    assert urlparse(account_trade_api.paths[0]).path == "/internal/v1/account/spot/trades"
    assert parse_qs(urlparse(account_trade_api.paths[0]).query) == {
        "user": [SUBACCOUNT],
        "orderId": [ORDER_ID],
        "orderSide": ["Sell"],
        "name": ["ETH/USDC"],
        "sort": ["DESC"],
        "pageSize": ["3"],
    }


def test_spot_account_trade_pages_follow_cursor(account_trade_api: SimpleNamespace) -> None:
    """Retain page boundaries and continue from the venue cursor."""
    pages = get_deepx_spot_account_trade_pages(
        SUBACCOUNT,
        2,
        name="ETH/USDC",
        page_size=3,
        config=account_trade_api.config,
    )

    assert [len(page.items) for page in pages] == [3, 0]
    assert not pages[1].has_next
    assert parse_qs(urlparse(account_trade_api.paths[1]).query)["cursor"] == [
        pages[0].next_cursor,
    ]


def test_spot_account_trades_reject_invalid_filters_before_network(
    account_trade_api: SimpleNamespace,
) -> None:
    """Require the documented order filter tuple and valid time bounds."""
    with pytest.raises(ValueError, match="order_id requires one market selector and order_side"):
        get_deepx_spot_account_trades(
            SUBACCOUNT,
            order_id=ORDER_ID,
            config=account_trade_api.config,
        )

    with pytest.raises(ValueError, match="start_ms must not exceed end_ms"):
        get_deepx_spot_account_trades(
            SUBACCOUNT,
            start_ms=2_000_000_000_000,
            end_ms=1_000_000_000_000,
            config=account_trade_api.config,
        )

    with pytest.raises(ValueError, match="max_pages must be greater than zero"):
        get_deepx_spot_account_trade_pages(SUBACCOUNT, 0, config=account_trade_api.config)

    assert account_trade_api.paths == []


def test_spot_account_trade_pages_reject_repeated_id(account_trade_api: SimpleNamespace) -> None:
    """Reject one repeated trade ID across pages without returning partial data."""
    account_trade_api.responses[1]["data"]["items"] = [
        account_trade_api.responses[0]["data"]["items"][-1],
    ]

    with pytest.raises(ValueError, match="repeats trade ID"):
        get_deepx_spot_account_trade_pages(
            SUBACCOUNT,
            2,
            name="ETH/USDC",
            page_size=3,
            config=account_trade_api.config,
        )
