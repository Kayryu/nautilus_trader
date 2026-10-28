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

"""Tests for read-only perpetual open-order Python bindings."""

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
from nautilus_trader.adapters.deepx import DeepXPerpOpenOrdersPage
from nautilus_trader.adapters.deepx import get_deepx_perp_open_order_pages
from nautilus_trader.adapters.deepx import get_deepx_perp_open_orders


SUBACCOUNT = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/deepx-rust-sdk/test_data/http/testnet/perp_history_orders_account_market_3.json"
)


@pytest.fixture
def open_order_api() -> Iterator[SimpleNamespace]:
    """Serve one structural order fixture and a terminating cursor page."""
    first = json.loads(FIXTURE.read_bytes())
    first["data"]["items"] = first["data"]["items"][:1]
    first["data"]["items"][0].update(
        status="Open",
        sizeFilled=0,
        sizeRemain=0.3,
        avgFillPrice=None,
        fee=0,
    )
    first["data"]["hasNext"] = True
    first["data"]["nextCursor"] = "next"
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


def test_perp_open_order_page_exposes_validated_exact_values(
    open_order_api: SimpleNamespace,
) -> None:
    """Expose the typed page without rounding the order's financial values."""
    page = get_deepx_perp_open_orders(SUBACCOUNT, 3, page_size=1, config=open_order_api.config)

    assert isinstance(page, DeepXPerpOpenOrdersPage)
    assert page.has_next
    assert page.next_cursor == "next"
    assert len(page.items) == 1
    assert page.items[0].order_id == "1789445193480"
    assert page.items[0].size == "0.3"
    assert page.items[0].price == "2523.683573"
    assert page.items[0].fee == "0"
    assert urlparse(open_order_api.paths[0]).path == "/internal/v1/account/perp/open-orders"
    assert parse_qs(urlparse(open_order_api.paths[0]).query) == {
        "user": [SUBACCOUNT],
        "marketId": ["3"],
        "sort": ["DESC"],
        "pageSize": ["1"],
    }


def test_perp_open_order_pages_follow_cursor(open_order_api: SimpleNamespace) -> None:
    """Forward the exact venue cursor within the caller's page budget."""
    pages = get_deepx_perp_open_order_pages(
        SUBACCOUNT,
        3,
        2,
        page_size=1,
        config=open_order_api.config,
    )

    assert [len(page.items) for page in pages] == [1, 0]
    assert not pages[1].has_next
    assert parse_qs(urlparse(open_order_api.paths[1]).query)["cursor"] == ["next"]


def test_perp_open_orders_reject_invalid_request_before_network(
    open_order_api: SimpleNamespace,
) -> None:
    """Reject invalid filters before sending a REST request."""
    with pytest.raises(ValueError, match="sort must be ASC or DESC"):
        get_deepx_perp_open_orders(SUBACCOUNT, 3, sort="INVALID", config=open_order_api.config)

    with pytest.raises(ValueError, match="market"):
        get_deepx_perp_open_orders(SUBACCOUNT, 0, config=open_order_api.config)

    with pytest.raises(ValueError, match="max_pages must be greater than zero"):
        get_deepx_perp_open_order_pages(SUBACCOUNT, 3, 0, config=open_order_api.config)

    assert open_order_api.paths == []
