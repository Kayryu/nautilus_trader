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

"""Tests for read-only wallet funding-fee observations."""

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
from nautilus_trader.adapters.deepx import DeepXPerpFundingFeeRecord
from nautilus_trader.adapters.deepx import DeepXWalletFundingFeesPage
from nautilus_trader.adapters.deepx import get_deepx_wallet_funding_fee_pages
from nautilus_trader.adapters.deepx import get_deepx_wallet_funding_fees


WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/test_data/http/testnet/wallet_funding_fees_page_2.json"
)


@pytest.fixture
def funding_api() -> Iterator[SimpleNamespace]:
    """Serve captured wallet funding fees from a local credential-free endpoint."""
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


def test_wallet_funding_fees_preserve_exact_values(funding_api: SimpleNamespace) -> None:
    """Expose exact signed values and the response's global cursor."""
    page = get_deepx_wallet_funding_fees(WALLET, market_id=3, config=funding_api.config)

    assert isinstance(page, DeepXWalletFundingFeesPage)
    assert page.has_next
    assert page.next_cursor
    assert len(page.items) == 2
    fee = page.items[0]
    assert isinstance(fee, DeepXPerpFundingFeeRecord)
    assert fee.owner == "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
    assert fee.market == 3
    assert fee.position_size == "0.238"
    assert fee.is_long
    assert fee.fee == "-0.00017"
    assert fee.fee_rate == "0.000489097868208476"
    assert fee.height == 187071212
    assert fee.event_idx == 13
    assert fee.is_settled
    assert urlparse(funding_api.paths[0]).path == "/internal/v1/account/funding-fee-by-wallet"
    assert parse_qs(urlparse(funding_api.paths[0]).query) == {
        "address": [WALLET],
        "marketId": ["3"],
    }


def test_wallet_funding_fee_pages_follow_global_cursor(funding_api: SimpleNamespace) -> None:
    """Collect terminal pages using the one wallet-wide cursor."""
    first = funding_api.responses[0]
    second = json.loads(json.dumps(first))
    second["data"]["items"] = []
    second["data"]["nextCursor"] = None
    second["data"]["hasNext"] = False
    funding_api.responses = [first, second]

    pages = get_deepx_wallet_funding_fee_pages(
        WALLET,
        2,
        market_id=3,
        config=funding_api.config,
    )

    assert len(pages) == 2
    assert len(pages[0].items) == 2
    assert pages[1].items == []
    assert not pages[1].has_next
    assert parse_qs(urlparse(funding_api.paths[1]).query)["cursor"] == [
        first["data"]["nextCursor"],
    ]


def test_wallet_funding_fee_pages_reject_duplicate_events(funding_api: SimpleNamespace) -> None:
    """Reject a mutable continuation that repeats an earlier event."""
    first = funding_api.responses[0]
    second = json.loads(json.dumps(first))
    second["data"]["nextCursor"] = None
    second["data"]["hasNext"] = False
    funding_api.responses = [first, second]

    with pytest.raises(ValueError, match="repeats event identity"):
        get_deepx_wallet_funding_fee_pages(WALLET, 2, market_id=3, config=funding_api.config)


def test_wallet_funding_fee_request_rejected_before_network(funding_api: SimpleNamespace) -> None:
    """Reject conflicting market selectors before opening a connection."""
    with pytest.raises(ValueError, match="not both"):
        get_deepx_wallet_funding_fees(
            WALLET,
            market_name="ETH-USDC",
            market_id=3,
            config=funding_api.config,
        )

    assert funding_api.paths == []
