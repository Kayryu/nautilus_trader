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

"""Tests for read-only grouped perpetual wallet orders."""

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
from nautilus_trader.adapters.deepx import DeepXPerpOrderRecord
from nautilus_trader.adapters.deepx import DeepXPerpWalletOrdersPage
from nautilus_trader.adapters.deepx import get_deepx_perp_wallet_order_pages
from nautilus_trader.adapters.deepx import get_deepx_perp_wallet_orders


WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/test_data/http/testnet/perp_wallet_orders_page_1.json"
)


@pytest.fixture
def perp_order_api() -> Iterator[SimpleNamespace]:
    """Serve the captured public wallet order page from loopback."""
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


def test_perp_wallet_orders_preserve_exact_values(perp_order_api: SimpleNamespace) -> None:
    """Expose validated groups and unrounded order decimals."""
    page = get_deepx_perp_wallet_orders(WALLET, page_size=5, config=perp_order_api.config)

    assert isinstance(page, DeepXPerpWalletOrdersPage)
    assert page.has_next
    assert len(page.markets) == 1
    market = page.markets[0]
    assert market.market_id == 3
    assert market.market_name == "ETH-USDC"
    assert len(market.subaccounts) == 2
    group = market.subaccounts[0]
    assert group.subaccount == "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
    assert group.orders.next_cursor == page.next_cursor
    assert len(group.orders.items) == 4
    order = group.orders.items[0]
    assert isinstance(order, DeepXPerpOrderRecord)
    assert order.order_id == "1789522185370"
    assert order.owner == group.subaccount
    assert order.market_id == 3
    assert not order.is_long
    assert order.size == "0.3"
    assert order.price == "2377.84827"
    assert order.avg_fill_price == "2403.9885999999997"
    assert order.size_filled == "0.3"
    assert order.size_remain == "0.0"
    assert order.fee == "-0.144111"
    assert order.take_profit is None
    assert order.status == "Filled"
    assert order.height == 187071212
    assert urlparse(perp_order_api.paths[0]).path == "/internal/v1/account/perp/orders-by-wallet"
    assert parse_qs(urlparse(perp_order_api.paths[0]).query) == {
        "address": [WALLET],
        "sort": ["DESC"],
        "pageSize": ["5"],
    }


def test_perp_wallet_order_pages_follow_global_cursor(perp_order_api: SimpleNamespace) -> None:
    """Continue with the cursor shared by all nested groups."""
    perp_order_api.responses.append({"code": 200, "msg": "success", "data": [], "fail": False})

    pages = get_deepx_perp_wallet_order_pages(WALLET, 2, page_size=5, config=perp_order_api.config)

    assert [len(page.markets) for page in pages] == [1, 0]
    assert not pages[1].has_next
    assert parse_qs(urlparse(perp_order_api.paths[1]).query)["cursor"] == [pages[0].next_cursor]


def test_perp_wallet_order_pages_reject_duplicate_ids(perp_order_api: SimpleNamespace) -> None:
    """Reject a repeated order identity across pages."""
    second = json.loads(json.dumps(perp_order_api.responses[0]))
    groups = second["data"][0]["subaccounts"]
    groups[0]["orders"]["items"] = groups[0]["orders"]["items"][-1:]
    groups[1]["orders"]["items"] = groups[1]["orders"]["items"][-1:]
    for group in groups:
        group["orders"]["nextCursor"] = None
        group["orders"]["hasNext"] = False
    perp_order_api.responses.append(second)

    with pytest.raises(ValueError, match="repeats wallet order identity"):
        get_deepx_perp_wallet_order_pages(WALLET, 2, page_size=5, config=perp_order_api.config)


def test_perp_wallet_orders_reject_invalid_filter_before_network(
    perp_order_api: SimpleNamespace,
) -> None:
    """Reject incompatible market filters without sending a request."""
    with pytest.raises(ValueError, match="market_name and market_id"):
        get_deepx_perp_wallet_orders(
            WALLET,
            market_name="ETH-USDC",
            market_id=3,
            config=perp_order_api.config,
        )

    assert perp_order_api.paths == []
