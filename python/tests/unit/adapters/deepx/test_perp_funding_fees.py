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

"""Tests for read-only subaccount perpetual funding-fee Python bindings."""

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
from nautilus_trader.adapters.deepx import DeepXPerpFundingFeesPage
from nautilus_trader.adapters.deepx import get_deepx_perp_funding_fee_pages
from nautilus_trader.adapters.deepx import get_deepx_perp_funding_fees


SUBACCOUNT = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/deepx-rust-sdk/test_data/http/testnet/perp_funding_fees_account_market_3.json"
)


@pytest.fixture
def funding_api() -> Iterator[SimpleNamespace]:
    """Serve the captured account funding page and an empty continuation."""
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


def test_perp_funding_fees_preserve_exact_signed_values(funding_api: SimpleNamespace) -> None:
    """Keep signed fee and rate decimals without converting their venue semantics."""
    page = get_deepx_perp_funding_fees(
        SUBACCOUNT,
        market_id=3,
        page_size=4,
        config=funding_api.config,
    )

    assert isinstance(page, DeepXPerpFundingFeesPage)
    assert len(page.items) == 4
    assert not page.has_next
    assert page.items[0].fee == "-0.00017"
    assert page.items[0].fee_rate == "0.000489097868208476"
    assert page.items[0].position_size == "0.238"
    assert page.items[0].is_settled
    assert urlparse(funding_api.paths[0]).path == "/internal/v1/account/funding-fee"
    assert parse_qs(urlparse(funding_api.paths[0]).query) == {
        "user": [SUBACCOUNT],
        "marketId": ["3"],
        "pageSize": ["4"],
    }


def test_perp_funding_fee_pages_follow_cursor(funding_api: SimpleNamespace) -> None:
    """Bound collection and retain the venue's cursor exactly."""
    funding_api.responses[0]["data"]["hasNext"] = True
    funding_api.responses[0]["data"]["nextCursor"] = "captured-cursor"

    pages = get_deepx_perp_funding_fee_pages(
        SUBACCOUNT,
        2,
        market_id=3,
        page_size=4,
        config=funding_api.config,
    )

    assert [len(page.items) for page in pages] == [4, 0]
    assert parse_qs(urlparse(funding_api.paths[1]).query)["cursor"] == ["captured-cursor"]


def test_perp_funding_fee_request_rejects_invalid_bounds(funding_api: SimpleNamespace) -> None:
    """Reject invalid ranges and zero collection budgets before network access."""
    with pytest.raises(ValueError, match="start_ms must not exceed end_ms"):
        get_deepx_perp_funding_fees(
            SUBACCOUNT,
            start_ms=2_000_000_000_000,
            end_ms=1_000_000_000_000,
            config=funding_api.config,
        )

    with pytest.raises(ValueError, match="max_pages must be greater than zero"):
        get_deepx_perp_funding_fee_pages(SUBACCOUNT, 0, config=funding_api.config)

    assert funding_api.paths == []


def test_perp_funding_fee_pages_reject_repeated_event(funding_api: SimpleNamespace) -> None:
    """Reject a repeated chain event across pages without returning partial data."""
    funding_api.responses[0]["data"]["hasNext"] = True
    funding_api.responses[0]["data"]["nextCursor"] = "captured-cursor"
    funding_api.responses[1]["data"]["items"] = [
        funding_api.responses[0]["data"]["items"][-1],
    ]

    with pytest.raises(ValueError, match="repeats event identity"):
        get_deepx_perp_funding_fee_pages(
            SUBACCOUNT,
            2,
            market_id=3,
            page_size=4,
            config=funding_api.config,
        )
