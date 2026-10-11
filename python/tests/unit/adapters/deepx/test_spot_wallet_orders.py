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

"""Tests for read-only grouped Spot wallet orders."""

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
from nautilus_trader.adapters.deepx import DeepXSpotOrderRecord
from nautilus_trader.adapters.deepx import DeepXSpotWalletOrdersPage
from nautilus_trader.adapters.deepx import get_deepx_spot_wallet_order_pages
from nautilus_trader.adapters.deepx import get_deepx_spot_wallet_orders


CAPTURED_WALLET = "0x89bb0946046588f4f257ffc71bd3231aba13474a"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/test_data/http/testnet/spot_wallet_orders_eth_usdc_page_1.json"
)


@pytest.fixture
def spot_order_api() -> Iterator[SimpleNamespace]:
    """Serve the captured public Spot wallet order page from loopback."""
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


def test_spot_wallet_orders_preserve_exact_values(spot_order_api: SimpleNamespace) -> None:
    """Expose validated order groups and original decimal values."""
    page = get_deepx_spot_wallet_orders(
        CAPTURED_WALLET,
        name="ETH/USDC",
        page_size=5,
        config=spot_order_api.config,
    )

    assert isinstance(page, DeepXSpotWalletOrdersPage)
    assert page.has_next
    assert len(page.markets) == 1
    market = page.markets[0]
    assert market.name == "ETH/USDC"
    group = market.subaccounts[0]
    assert group.subaccount == "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c"
    assert group.orders.next_cursor == page.next_cursor
    assert len(group.orders.items) == 5
    order = group.orders.items[0]
    assert isinstance(order, DeepXSpotOrderRecord)
    assert order.order_id == "1789631509620"
    assert order.order_side == "Sell"
    assert order.maker == group.subaccount
    assert order.price == "2563.57"
    assert order.avg_fill_price is None
    assert order.base_amount == "0.7329"
    assert order.quote_amount == "1878.840453"
    assert order.base_remaining_amount == "0.7329"
    assert order.status == "Canceled"
    assert order.cancel_reason == "UserCanceled"
    assert order.cancel_height == 188632838
    assert order.fee == "0.0"
    assert urlparse(spot_order_api.paths[0]).path == "/internal/v1/account/spot/orders-by-wallet"
    assert parse_qs(urlparse(spot_order_api.paths[0]).query) == {
        "address": [CAPTURED_WALLET],
        "name": ["ETH/USDC"],
        "sort": ["DESC"],
        "pageSize": ["5"],
    }


def test_spot_wallet_order_pages_follow_global_cursor(spot_order_api: SimpleNamespace) -> None:
    """Continue with the one cursor shared by all nonempty groups."""
    first = spot_order_api.responses[0]
    second = json.loads(json.dumps(first))
    second["data"][0]["subaccounts"][0]["orders"] = {
        "items": [],
        "nextCursor": None,
        "hasNext": False,
    }
    spot_order_api.responses = [first, second]

    pages = get_deepx_spot_wallet_order_pages(
        CAPTURED_WALLET,
        2,
        name="ETH/USDC",
        page_size=5,
        config=spot_order_api.config,
    )

    assert len(pages) == 2
    assert len(pages[0].markets[0].subaccounts[0].orders.items) == 5
    assert not pages[1].has_next
    assert parse_qs(urlparse(spot_order_api.paths[1]).query)["cursor"] == [
        pages[0].next_cursor,
    ]


def test_spot_wallet_order_pages_reject_duplicate_ids(spot_order_api: SimpleNamespace) -> None:
    """Reject an order repeated on a mutable continuation page."""
    first = spot_order_api.responses[0]
    second = json.loads(json.dumps(first))
    orders = second["data"][0]["subaccounts"][0]["orders"]
    orders["items"] = orders["items"][-1:]
    orders["nextCursor"] = None
    orders["hasNext"] = False
    spot_order_api.responses = [first, second]

    with pytest.raises(ValueError, match="repeats wallet order identity"):
        get_deepx_spot_wallet_order_pages(
            CAPTURED_WALLET,
            2,
            name="ETH/USDC",
            page_size=5,
            config=spot_order_api.config,
        )


def test_spot_wallet_order_request_rejected_before_network(spot_order_api: SimpleNamespace) -> None:
    """Reject an invalid side before contacting the venue."""
    with pytest.raises(ValueError, match="order_side must be Buy or Sell"):
        get_deepx_spot_wallet_orders(
            CAPTURED_WALLET,
            order_side="Other",
            config=spot_order_api.config,
        )

    assert spot_order_api.paths == []
