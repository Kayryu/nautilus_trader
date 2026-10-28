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
"""Tests for bounded public DeepX trade loading into replayable ticks."""

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
from nautilus_trader.adapters.deepx import load_deepx_perp_trade_ticks
from nautilus_trader.adapters.deepx import load_deepx_spot_trade_ticks


MARKETS = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/deepx-rust-sdk/test_data/http/testnet/perp_markets.json"
).read_bytes()
SPOT_MARKETS = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/deepx-rust-sdk/test_data/http/testnet/spot_markets.json"
).read_bytes()
START_MS = 1_788_220_800_000
END_MS = START_MS + 600_000


@pytest.fixture
def market_api() -> Iterator[SimpleNamespace]:
    """Serve market metadata and one controlled trade page."""
    response = SimpleNamespace(paths=[], trade_body=b"")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            response.paths.append(self.path)
            path = urlparse(self.path).path
            if path == "/internal/v1/market/perp/markets":
                body = MARKETS
            elif path == "/internal/v1/market/spot/markets":
                body = SPOT_MARKETS
            elif path == "/internal/v1/market/perp/trades":
                body = response.trade_body
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            response.paths.append(f"RPC {request['method']}")
            body = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": "0x" + "11" * 32,
                },
            ).encode()
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
    response.config = DeepXDataClientConfig(
        network=DeepXNetworkConfig(
            base_url_rest=f"http://127.0.0.1:{server.server_port}",
            base_url_rpc=f"http://127.0.0.1:{server.server_port}",
        ),
    )
    try:
        yield response
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def trade(trade_id: int, time: str, price: str) -> dict[str, object]:
    """Build one complete public trade record."""
    return {
        "id": trade_id,
        "marketId": 3,
        "buyerOrderId": "1",
        "buyer": "0x1111111111111111111111111111111111111111",
        "sellerOrderId": "2",
        "seller": "0x2222222222222222222222222222222222222222",
        "price": price,
        "size": "0.0120",
        "buyerLeverage": "10",
        "sellerLeverage": "10",
        "createdAt": time,
        "filledDirection": "Long",
        "taker": "Buyer",
        "takerFee": "0.001",
        "makerFee": "-0.0005",
    }


def test_loads_exact_chronological_trade_ticks(market_api: SimpleNamespace) -> None:
    """Return exact trade values in replay order with event-aligned initialization."""
    market_api.trade_body = json.dumps(
        {
            "code": 200,
            "msg": "success",
            "fail": False,
            "data": {
                "items": [
                    trade(2, "2026-09-01T00:09:00.000Z", "2435.0100"),
                    trade(1, "2026-09-01T00:08:00.000Z", "2435.0000"),
                ],
                "nextCursor": None,
                "hasNext": False,
            },
        },
    ).encode()

    instrument, ticks = load_deepx_perp_trade_ticks(
        3,
        START_MS,
        END_MS,
        limit=2,
        max_pages=1,
        config=market_api.config,
    )

    assert str(instrument.id) == "ETH-USDC-PERP.DEEPX"
    assert [str(tick.trade_id) for tick in ticks] == ["1", "2"]
    assert [str(tick.price) for tick in ticks] == ["2435.0000", "2435.0100"]
    assert all(tick.ts_init == tick.ts_event for tick in ticks)
    assert len(market_api.paths) == 2
    trade_path = urlparse(market_api.paths[1])
    assert trade_path.path == "/internal/v1/market/perp/trades"
    assert parse_qs(trade_path.query)["marketId"] == ["3"]


def test_rejects_precision_loss_without_partial_result(market_api: SimpleNamespace) -> None:
    """Reject a trade outside the validated market's price precision."""
    market_api.trade_body = json.dumps(
        {
            "code": 200,
            "msg": "success",
            "fail": False,
            "data": {
                "items": [trade(1, "2026-09-01T00:08:00.000Z", "2435.00001")],
                "nextCursor": None,
                "hasNext": False,
            },
        },
    ).encode()

    with pytest.raises(ValueError, match="would lose precision"):
        load_deepx_perp_trade_ticks(
            3,
            START_MS,
            END_MS,
            limit=1,
            max_pages=1,
            config=market_api.config,
        )


@pytest.mark.parametrize(
    ("limit", "max_pages", "message"),
    [(0, 1, "nonzero"), (10_001, 1, "10000"), (1, 0, "max_pages")],
)
def test_rejects_invalid_budgets_before_network(
    limit: int,
    max_pages: int,
    message: str,
) -> None:
    """Reject invalid read limits without contacting a backend."""
    with pytest.raises(ValueError, match=message):
        load_deepx_perp_trade_ticks(3, START_MS, END_MS, limit=limit, max_pages=max_pages)


@pytest.mark.parametrize(
    ("limit", "max_pages", "message"),
    [(0, 1, "nonzero"), (10_001, 1, "10000"), (1, 0, "max_pages")],
)
def test_spot_loader_rejects_invalid_budgets_before_network(
    limit: int,
    max_pages: int,
    message: str,
) -> None:
    """Reject invalid Spot history limits before market or chain discovery."""
    with pytest.raises(ValueError, match=message):
        load_deepx_spot_trade_ticks(
            "ETH/USDC",
            START_MS,
            END_MS,
            limit=limit,
            max_pages=max_pages,
        )


def test_spot_loader_requires_spot_discovery() -> None:
    """Reject a config that explicitly excludes Spot instrument metadata."""
    with pytest.raises(ValueError, match="Spot discovery"):
        load_deepx_spot_trade_ticks(
            "ETH/USDC",
            START_MS,
            END_MS,
            config=DeepXDataClientConfig(perpetual_only=True),
        )


def test_spot_loader_rejects_unapproved_chain_before_trades(
    market_api: SimpleNamespace,
) -> None:
    """Require an approved RPC identity before returning Spot history or an instrument."""
    with pytest.raises(ValueError, match="genesis hash is not approved"):
        load_deepx_spot_trade_ticks(
            "ETH/USDC",
            START_MS,
            END_MS,
            config=market_api.config,
        )

    assert "RPC chain_getBlockHash" in market_api.paths
    assert all("/market/spot/trades" not in path for path in market_api.paths)
