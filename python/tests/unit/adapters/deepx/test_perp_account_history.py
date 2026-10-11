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

"""Tests for read-only Perp subaccount order and trade history bindings."""

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
from nautilus_trader.adapters.deepx import DeepXPerpAccountTradesPage
from nautilus_trader.adapters.deepx import DeepXPerpHistoryOrdersPage
from nautilus_trader.adapters.deepx import get_deepx_perp_account_trade_pages
from nautilus_trader.adapters.deepx import get_deepx_perp_account_trades
from nautilus_trader.adapters.deepx import get_deepx_perp_history_order_pages
from nautilus_trader.adapters.deepx import get_deepx_perp_history_orders


SUBACCOUNT = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
TEST_DATA = Path(__file__).resolve().parents[5] / "crates/adapters/deepx/test_data/http/testnet"
ORDERS_PATH = "/internal/v1/account/perp/history-orders"
TRADES_PATH = "/internal/v1/account/perp/trades"


@pytest.fixture
def account_api() -> Iterator[SimpleNamespace]:
    """Serve captured testnet pages and optional continuations on loopback."""
    state = SimpleNamespace(
        responses={
            ORDERS_PATH: [
                json.loads((TEST_DATA / "perp_history_orders_account_market_3.json").read_bytes()),
            ],
            TRADES_PATH: [
                json.loads((TEST_DATA / "perp_account_trades_account_market_3.json").read_bytes()),
            ],
        },
        paths=[],
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.paths.append(self.path)
            path = urlparse(self.path).path
            body = json.dumps(state.responses[path].pop(0)).encode()
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


def test_perp_history_orders_preserve_terminal_values(account_api: SimpleNamespace) -> None:
    """Expose captured filled orders with exact decimal strings."""
    page = get_deepx_perp_history_orders(
        SUBACCOUNT,
        market_id=3,
        page_size=3,
        config=account_api.config,
    )

    assert isinstance(page, DeepXPerpHistoryOrdersPage)
    assert len(page.items) == 3
    assert not page.has_next
    assert page.items[0].status == "Filled"
    assert page.items[0].order_id == "1789445193480"
    assert page.items[0].avg_fill_price == "2499.6"
    assert page.items[0].fee == "-0.149921"
    assert parse_qs(urlparse(account_api.paths[0]).query) == {
        "user": [SUBACCOUNT],
        "marketId": ["3"],
        "sort": ["DESC"],
        "pageSize": ["3"],
    }


def test_perp_account_trades_preserve_signed_fee(account_api: SimpleNamespace) -> None:
    """Expose exact account trade values without inferring fill semantics."""
    page = get_deepx_perp_account_trades(
        SUBACCOUNT,
        market_id=3,
        page_size=3,
        config=account_api.config,
    )

    assert isinstance(page, DeepXPerpAccountTradesPage)
    assert len(page.items) == 3
    assert not page.has_next
    assert page.items[0].id == 185971383000008
    assert page.items[0].order_id == "1789445193480"
    assert page.items[0].price == "2499.6"
    assert page.items[0].fee == "-0.149921"
    assert urlparse(account_api.paths[0]).path == TRADES_PATH
    assert parse_qs(urlparse(account_api.paths[0]).query) == {
        "user": [SUBACCOUNT],
        "marketId": ["3"],
        "sort": ["DESC"],
        "pageSize": ["3"],
    }


@pytest.mark.parametrize("path", [ORDERS_PATH, TRADES_PATH])
def test_perp_account_pages_follow_cursor(account_api: SimpleNamespace, path: str) -> None:
    """Continue from the exact venue cursor within the caller's page budget."""
    first = account_api.responses[path][0]
    first["data"]["hasNext"] = True
    first["data"]["nextCursor"] = "captured-cursor"
    account_api.responses[path].append(
        {
            "code": 200,
            "msg": "success",
            "data": {"items": [], "hasNext": False, "nextCursor": None},
            "fail": False,
        },
    )

    if path == ORDERS_PATH:
        pages = get_deepx_perp_history_order_pages(
            SUBACCOUNT,
            2,
            market_id=3,
            page_size=3,
            config=account_api.config,
        )
    else:
        pages = get_deepx_perp_account_trade_pages(
            SUBACCOUNT,
            2,
            market_id=3,
            page_size=3,
            config=account_api.config,
        )

    assert [len(page.items) for page in pages] == [3, 0]
    assert parse_qs(urlparse(account_api.paths[1]).query)["cursor"] == ["captured-cursor"]


def test_perp_account_requests_reject_invalid_filters_before_network(
    account_api: SimpleNamespace,
) -> None:
    """Require the documented trade filter tuple and positive page budgets."""
    with pytest.raises(ValueError, match="order_id requires market_id and is_long"):
        get_deepx_perp_account_trades(
            SUBACCOUNT,
            order_id="1789445193480",
            market_id=3,
            config=account_api.config,
        )

    with pytest.raises(ValueError, match="max_pages must be greater than zero"):
        get_deepx_perp_history_order_pages(SUBACCOUNT, 0, config=account_api.config)

    assert account_api.paths == []
