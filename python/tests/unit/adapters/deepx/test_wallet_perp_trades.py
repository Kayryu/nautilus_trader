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

"""Tests for read-only wallet-grouped DeepX perpetual trade observations."""

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
from nautilus_trader.adapters.deepx import DeepXPerpAccountTradeRecord
from nautilus_trader.adapters.deepx import DeepXPerpWalletTradeMarket
from nautilus_trader.adapters.deepx import DeepXPerpWalletTradesPage
from nautilus_trader.adapters.deepx import get_deepx_perp_wallet_trade_pages
from nautilus_trader.adapters.deepx import get_deepx_perp_wallet_trades


WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
SUBACCOUNT = "0x977654069311fa41b88f7901dc65fae9a76fd60b"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/deepx-rust-sdk/test_data/http/testnet/perp_wallet_trades_page.json"
)


@pytest.fixture
def wallet_trade_api() -> Iterator[SimpleNamespace]:
    """Serve captured trade groups from a local credential-free endpoint."""
    state = SimpleNamespace(responses=[FIXTURE.read_bytes()], paths=[])

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.paths.append(self.path)
            body = state.responses.pop(0)
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


def test_wallet_trades_preserve_group_cursors_and_exact_values(
    wallet_trade_api: SimpleNamespace,
) -> None:
    """Expose per-subaccount cursors and raw decimal values without fill conversion."""
    markets = get_deepx_perp_wallet_trades(WALLET, page_size=5, config=wallet_trade_api.config)

    assert len(markets) == 2
    assert isinstance(markets[0], DeepXPerpWalletTradeMarket)
    assert markets[0].market_id == 3
    assert markets[0].market_name == "ETH-USDC"
    assert len(markets[0].subaccounts) == 3
    selected = next(group for group in markets[0].subaccounts if group.subaccount == SUBACCOUNT)
    assert selected.trades.has_next
    assert selected.trades.next_cursor is not None
    assert len(selected.trades.items) == 5
    first = markets[0].subaccounts[0].trades.items[0]
    assert isinstance(first, DeepXPerpAccountTradeRecord)
    assert first.order_id == "1789522185370"
    assert first.price == "2403.97"
    assert first.size == "0.238"
    assert first.fee == "-0.114328"
    assert first.fee_asset == ""
    assert markets[1].subaccounts[0].trades.items[2].size == "0.0"
    assert [urlparse(path).path for path in wallet_trade_api.paths] == [
        "/internal/v1/account/perp/trades-by-wallet",
    ]
    assert parse_qs(urlparse(wallet_trade_api.paths[0]).query) == {
        "address": [WALLET],
        "sort": ["DESC"],
        "pageSize": ["5"],
    }


def test_wallet_trade_pages_follow_only_selected_group_cursor(
    wallet_trade_api: SimpleNamespace,
) -> None:
    """Collect the selected subaccount without treating other group cursors as global."""
    first = json.loads(FIXTURE.read_bytes())
    first["data"] = first["data"][:1]
    group = next(
        group for group in first["data"][0]["subaccounts"] if group["subaccount"] == SUBACCOUNT
    )
    cursor = group["trades"]["nextCursor"]
    terminal = json.loads(json.dumps(first))
    terminal_group = next(
        group for group in terminal["data"][0]["subaccounts"] if group["subaccount"] == SUBACCOUNT
    )
    terminal_group["trades"] = {"items": [], "nextCursor": None, "hasNext": False}
    wallet_trade_api.responses = [json.dumps(first).encode(), json.dumps(terminal).encode()]

    pages = get_deepx_perp_wallet_trade_pages(
        WALLET,
        SUBACCOUNT,
        3,
        "ETH-USDC",
        2,
        page_size=5,
        config=wallet_trade_api.config,
    )

    assert len(pages) == 2
    assert isinstance(pages[0], DeepXPerpWalletTradesPage)
    assert len(pages[0].items) == 5
    assert pages[0].next_cursor == cursor
    assert pages[1].items == []
    assert not pages[1].has_next
    assert len(wallet_trade_api.paths) == 2
    assert "cursor" not in parse_qs(urlparse(wallet_trade_api.paths[0]).query)
    assert parse_qs(urlparse(wallet_trade_api.paths[1]).query)["cursor"] == [cursor]


def test_wallet_trade_pages_reject_exhausted_budget(wallet_trade_api: SimpleNamespace) -> None:
    """Return no partial pages when a selected group needs another request."""
    first = json.loads(FIXTURE.read_bytes())
    first["data"] = first["data"][:1]
    wallet_trade_api.responses = [json.dumps(first).encode()]

    with pytest.raises(ValueError, match="pagination exceeded its limit of 1 pages"):
        get_deepx_perp_wallet_trade_pages(
            WALLET,
            SUBACCOUNT,
            3,
            "ETH-USDC",
            1,
            page_size=5,
            config=wallet_trade_api.config,
        )

    assert len(wallet_trade_api.paths) == 1


def test_wallet_trades_reject_invalid_request_before_network(
    wallet_trade_api: SimpleNamespace,
) -> None:
    """Reject conflicting market selectors before contacting the venue."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        get_deepx_perp_wallet_trades(
            WALLET,
            market_name="ETH-USDC",
            market_id=3,
            config=wallet_trade_api.config,
        )

    assert wallet_trade_api.paths == []
