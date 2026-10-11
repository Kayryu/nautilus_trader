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

"""Tests for read-only Spot history-order Python bindings."""

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
from nautilus_trader.adapters.deepx import DeepXSpotHistoryOrdersPage
from nautilus_trader.adapters.deepx import get_deepx_spot_history_order_pages
from nautilus_trader.adapters.deepx import get_deepx_spot_history_orders


SUBACCOUNT = "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/test_data/http/testnet/spot_history_orders_eth_usdc_page_1.json"
)


@pytest.fixture
def history_api() -> Iterator[SimpleNamespace]:
    """Serve the captured history page and an empty continuation."""
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


def test_spot_history_orders_allow_cross_market_read(history_api: SimpleNamespace) -> None:
    """Preserve exact values without requiring a market selector."""
    history_api.responses[0]["data"]["items"][0]["status"] = "Canceled"

    page = get_deepx_spot_history_orders(
        SUBACCOUNT,
        page_size=3,
        config=history_api.config,
    )

    assert isinstance(page, DeepXSpotHistoryOrdersPage)
    assert page.has_next
    assert len(page.items) == 3
    assert page.items[0].status == "Canceled"
    assert page.items[0].price == "2456.05"
    assert page.items[0].base_remaining_amount == "0.8849"
    assert urlparse(history_api.paths[0]).path == "/internal/v1/account/spot/history-orders"
    assert parse_qs(urlparse(history_api.paths[0]).query) == {
        "user": [SUBACCOUNT],
        "sort": ["DESC"],
        "pageSize": ["3"],
    }


def test_spot_history_order_pages_follow_cursor(history_api: SimpleNamespace) -> None:
    """Collect only the requested number of validated pages."""
    pages = get_deepx_spot_history_order_pages(
        SUBACCOUNT,
        2,
        name="ETH/USDC",
        page_size=3,
        config=history_api.config,
    )

    assert [len(page.items) for page in pages] == [3, 0]
    assert not pages[1].has_next
    assert parse_qs(urlparse(history_api.paths[1]).query)["cursor"] == [pages[0].next_cursor]


def test_spot_history_orders_reject_invalid_request_before_network(
    history_api: SimpleNamespace,
) -> None:
    """Reject conflicting market scope and zero page budget locally."""
    with pytest.raises(ValueError, match="name or bytes32 pair"):
        get_deepx_spot_history_orders(
            SUBACCOUNT,
            name="ETH/USDC",
            pair="0x" + "00" * 32,
            config=history_api.config,
        )

    with pytest.raises(ValueError, match="max_pages must be greater than zero"):
        get_deepx_spot_history_order_pages(SUBACCOUNT, 0, config=history_api.config)

    assert history_api.paths == []


def test_spot_history_order_pages_reject_duplicate_ids(history_api: SimpleNamespace) -> None:
    """Reject a repeated order ID across pages without exposing partial history."""
    history_api.responses[1]["data"]["items"] = [
        history_api.responses[0]["data"]["items"][-1],
    ]

    with pytest.raises(ValueError, match="repeats order ID"):
        get_deepx_spot_history_order_pages(
            SUBACCOUNT,
            2,
            name="ETH/USDC",
            page_size=3,
            config=history_api.config,
        )
