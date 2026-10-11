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

"""Tests for read-only Spot open-order Python bindings."""

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
from nautilus_trader.adapters.deepx import DeepXSpotOpenOrdersPage
from nautilus_trader.adapters.deepx import get_deepx_spot_open_order_pages
from nautilus_trader.adapters.deepx import get_deepx_spot_open_orders


SUBACCOUNT = "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/test_data/http/testnet/spot_history_orders_eth_usdc_page_1.json"
)


@pytest.fixture
def open_order_api() -> Iterator[SimpleNamespace]:
    """Serve the captured active Spot page and an empty continuation."""
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


def test_spot_open_order_page_preserves_exact_values(open_order_api: SimpleNamespace) -> None:
    """Expose validated active records with exact decimal strings."""
    page = get_deepx_spot_open_orders(
        SUBACCOUNT,
        name="ETH/USDC",
        order_side="Sell",
        page_size=3,
        config=open_order_api.config,
    )

    assert isinstance(page, DeepXSpotOpenOrdersPage)
    assert page.has_next
    assert len(page.items) == 3
    assert page.items[0].status == "Open"
    assert page.items[0].order_id == "1789627379350"
    assert page.items[0].price == "2456.05"
    assert page.items[0].base_remaining_amount == "0.8849"
    assert urlparse(open_order_api.paths[0]).path == "/internal/v1/account/spot/open-orders"
    assert parse_qs(urlparse(open_order_api.paths[0]).query) == {
        "name": ["ETH/USDC"],
        "user": [SUBACCOUNT],
        "orderSide": ["Sell"],
        "sort": ["DESC"],
        "pageSize": ["3"],
    }


def test_spot_open_order_pages_follow_cursor(open_order_api: SimpleNamespace) -> None:
    """Keep the exact venue cursor within the caller's page limit."""
    pages = get_deepx_spot_open_order_pages(
        SUBACCOUNT,
        2,
        name="ETH/USDC",
        page_size=3,
        config=open_order_api.config,
    )

    assert [len(page.items) for page in pages] == [3, 0]
    assert not pages[1].has_next
    assert parse_qs(urlparse(open_order_api.paths[1]).query)["cursor"] == [pages[0].next_cursor]


def test_spot_open_orders_reject_invalid_request_before_network(
    open_order_api: SimpleNamespace,
) -> None:
    """Reject missing or conflicting market scope and invalid page limits locally."""
    with pytest.raises(ValueError, match="exactly one nonempty name or bytes32 pair"):
        get_deepx_spot_open_orders(SUBACCOUNT, config=open_order_api.config)

    with pytest.raises(ValueError, match="exactly one nonempty name or bytes32 pair"):
        get_deepx_spot_open_orders(
            SUBACCOUNT,
            name="ETH/USDC",
            pair="0x" + "00" * 32,
            config=open_order_api.config,
        )

    with pytest.raises(ValueError, match="max_pages must be greater than zero"):
        get_deepx_spot_open_order_pages(
            SUBACCOUNT,
            0,
            name="ETH/USDC",
            config=open_order_api.config,
        )

    assert open_order_api.paths == []


def test_spot_open_orders_reject_terminal_record(open_order_api: SimpleNamespace) -> None:
    """Never expose a terminal historical record as an active order."""
    open_order_api.responses[0]["data"]["items"][0]["status"] = "Canceled"

    with pytest.raises(ValueError, match="invalid active state or quantities"):
        get_deepx_spot_open_orders(
            SUBACCOUNT,
            name="ETH/USDC",
            page_size=3,
            config=open_order_api.config,
        )
