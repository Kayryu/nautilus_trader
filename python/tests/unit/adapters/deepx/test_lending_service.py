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

"""Tests for public read-only DeepX lending observations."""

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
from nautilus_trader.adapters.deepx import DeepXLendingService
from nautilus_trader.adapters.deepx import DeepXNetworkConfig


FIXTURES = Path(__file__).resolve().parents[5] / "crates/adapters/deepx/test_data/http/testnet"
PREFIX = "/internal/v1/market/lending"
START_MS = 1_789_430_000_000


@pytest.fixture
def lending_api() -> Iterator[SimpleNamespace]:
    """Serve captured public lending market responses from loopback."""
    state = SimpleNamespace(
        responses={
            f"{PREFIX}/assets": json.loads((FIXTURES / "lending_assets.json").read_bytes()),
            f"{PREFIX}/interest-rate": json.loads(
                (FIXTURES / "lending_interest_rate_usdc.json").read_bytes()
            ),
            f"{PREFIX}/interest-rate-history": json.loads(
                (FIXTURES / "lending_interest_rate_history_usdc_1h.json").read_bytes()
            ),
            f"{PREFIX}/status-history": json.loads(
                (FIXTURES / "lending_status_history_usdc_1h.json").read_bytes()
            ),
        },
        paths=[],
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.paths.append(self.path)
            body = json.dumps(state.responses[urlparse(self.path).path]).encode()
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


def test_lending_service_preserves_exact_public_market_values(lending_api: SimpleNamespace) -> None:
    """Expose all four validated lending response families without rounding."""
    service = DeepXLendingService(lending_api.config)
    assets = service.get_assets()
    params = service.get_interest_rate_params(market_id=1, asset="USDC")
    rates = service.get_interest_rate_history(
        start_ms=START_MS,
        market_id=1,
        asset="usdc",
        limit=3,
    )
    statuses = service.get_status_history(
        start_ms=START_MS,
        market_id=1,
        asset="usdc",
        limit=3,
    )

    assert [(asset.market_id, asset.asset) for asset in assets] == [
        (1, "eth"),
        (1, "sol"),
        (1, "usdc"),
    ]
    assert assets[2].height == 110143
    assert len(params) == 1
    assert params[0].u1 == "0.8"
    assert params[0].u4 is None
    assert params[0].r_max == "1.5"
    assert params[0].rho == "0.1"
    assert len(rates) == len(statuses) == 3
    assert rates[0].supply_apr == "0.00002115040506874738"
    assert rates[0].borrow_apr == "0.001327604517817316"
    assert rates[0].statistic_time == 1789552800000
    assert statuses[0].index_price == "1"
    assert statuses[0].total_supplied == "830055808.591571"
    assert statuses[0].total_borrowed == "14693144.553689"
    assert statuses[0].utilization_rate == "0.017701393570897547"
    assert [urlparse(path).path for path in lending_api.paths] == [
        f"{PREFIX}/assets",
        f"{PREFIX}/interest-rate",
        f"{PREFIX}/interest-rate-history",
        f"{PREFIX}/status-history",
    ]
    assert parse_qs(urlparse(lending_api.paths[1]).query) == {
        "marketId": ["1"],
        "asset": ["USDC"],
    }
    for path in lending_api.paths[2:]:
        assert parse_qs(urlparse(path).query) == {
            "marketId": ["1"],
            "asset": ["usdc"],
            "timeFrame": ["1h"],
            "start": [str(START_MS)],
            "limit": ["3"],
            "sort": ["DESC"],
        }


def test_lending_service_rejects_invalid_request_before_network(
    lending_api: SimpleNamespace,
) -> None:
    """Validate market, interval, and time bounds before a GET."""
    service = DeepXLendingService(lending_api.config)

    with pytest.raises(ValueError, match="market_id must be greater than zero"):
        service.get_interest_rate_params(market_id=0)
    with pytest.raises(ValueError, match="unsupported lending history interval"):
        service.get_interest_rate_history(start_ms=START_MS, interval="2M")
    with pytest.raises(ValueError, match="start_ms must not exceed end_ms"):
        service.get_status_history(start_ms=START_MS, end_ms=START_MS - 1)

    assert lending_api.paths == []


def test_lending_service_rejects_invalid_market_values(lending_api: SimpleNamespace) -> None:
    """Retain Rust's response validation through the Python boundary."""
    service = DeepXLendingService(lending_api.config)
    response = lending_api.responses[f"{PREFIX}/status-history"]
    response["data"]["details"][0]["utilization_rate"] = "1.1"

    with pytest.raises(ValueError, match="invalid financial values"):
        service.get_status_history(start_ms=START_MS, market_id=1, asset="usdc", limit=3)
