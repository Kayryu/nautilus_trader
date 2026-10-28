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

"""Tests for read-only hourly unsettled-funding boundaries."""

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
from nautilus_trader.adapters.deepx import DeepXHourlyUnsettledFundingRecord
from nautilus_trader.adapters.deepx import DeepXNetworkConfig
from nautilus_trader.adapters.deepx import get_deepx_hourly_unsettled_funding_pages


WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
FIXTURES = Path(__file__).resolve().parents[5] / "crates/adapters/deepx/deepx-rust-sdk/test_data/http/testnet"
START_MS = 1_789_488_862_441
END_MS = 1_789_521_262_677


@pytest.fixture
def funding_api() -> Iterator[SimpleNamespace]:
    """Serve captured wallet funding boundaries from loopback."""
    first = json.loads(
        (FIXTURES / "wallet_hourly_unsettled_funding_market_3_page_1.json").read_bytes()
    )
    second = json.loads(
        (FIXTURES / "wallet_hourly_unsettled_funding_market_3_page_2.json").read_bytes()
    )
    state = SimpleNamespace(
        responses=[first, second, {"code": 200, "msg": "success", "data": [], "fail": False}],
        paths=[],
    )

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


def test_hourly_funding_preserves_exact_raw_units_and_keyset(funding_api: SimpleNamespace) -> None:
    """Expose exact raw integers and follow the complete keyset cursor."""
    pages = get_deepx_hourly_unsettled_funding_pages(
        max_pages=3,
        wallet=WALLET,
        market_id=3,
        start_ms=START_MS,
        end_ms=END_MS,
        page_size=5,
        config=funding_api.config,
    )

    assert [len(page) for page in pages] == [5, 5, 0]
    record = pages[0][0]
    assert isinstance(record, DeepXHourlyUnsettledFundingRecord)
    assert record.subaccount == "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
    assert record.market_id == 3
    assert record.signed_position_size_raw == "300000000000000000"
    assert record.baseline_index_raw == "77064622395991726"
    assert record.cumulative_index_raw == "77077122395991726"
    assert record.delta_index_raw == "12500000000000"
    assert record.mark_price_raw == "2397824333"
    assert record.payment_raw == "-8991"
    assert record.boundary_block == 187058004
    assert record.boundary_event_index == 14
    assert urlparse(funding_api.paths[0]).path == "/internal/v1/account/hourly-unsettled-funding"
    assert parse_qs(urlparse(funding_api.paths[0]).query) == {
        "wallet": [WALLET],
        "marketId": ["3"],
        "start": [str(START_MS)],
        "end": [str(END_MS)],
        "pageSize": ["5"],
        "sort": ["DESC"],
    }
    first_last = pages[0][-1]
    assert parse_qs(urlparse(funding_api.paths[1]).query) == {
        **parse_qs(urlparse(funding_api.paths[0]).query),
        "cursorTimestamp": [str(first_last.boundary_timestamp_ms)],
        "cursorMarketId": [str(first_last.market_id)],
        "cursorSubaccount": [first_last.subaccount],
        "cursorEventId": [first_last.boundary_event_id],
    }


def test_hourly_funding_rejects_page_budget_exhaustion(funding_api: SimpleNamespace) -> None:
    """A full final page requires a further empty terminal request."""
    with pytest.raises(ValueError, match="pagination exceeded its limit of 2 pages"):
        get_deepx_hourly_unsettled_funding_pages(
            max_pages=2,
            wallet=WALLET,
            market_id=3,
            start_ms=START_MS,
            end_ms=END_MS,
            page_size=5,
            config=funding_api.config,
        )

    assert len(funding_api.paths) == 2


def test_hourly_funding_rejects_invalid_scope_before_network(funding_api: SimpleNamespace) -> None:
    """Require exactly one account scope before opening a connection."""
    with pytest.raises(ValueError, match="exactly one subaccount or wallet"):
        get_deepx_hourly_unsettled_funding_pages(
            max_pages=1,
            config=funding_api.config,
        )

    assert funding_api.paths == []
