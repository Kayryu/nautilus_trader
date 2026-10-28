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

"""Tests for read-only transaction-hash order associations."""

import json
from collections.abc import Callable
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
from nautilus_trader.adapters.deepx import DeepXSpotOrderRecord
from nautilus_trader.adapters.deepx import get_deepx_perp_order_by_tx
from nautilus_trader.adapters.deepx import get_deepx_spot_order_by_tx


FIXTURES = Path(__file__).resolve().parents[5] / "crates/adapters/deepx/deepx-rust-sdk/test_data/http/testnet"
PERP_HASH = "0x247ef3967338c6985714731a49752455ebe877d8678e0b261b3638892cfae554"
SPOT_HASH = "0x437279a6138febacf1a416ad24e3377f8c1ecd4ee0f9cdd6702f11f20d9e1750"


@pytest.fixture
def order_api() -> Iterator[SimpleNamespace]:
    """Serve captured order associations from loopback."""
    state = SimpleNamespace(
        responses={
            "/internal/v1/account/perp/order-by-tx": json.loads(
                (FIXTURES / "perp_order_by_tx.json").read_bytes()
            ),
            "/internal/v1/account/spot/order-by-tx": json.loads(
                (FIXTURES / "spot_order_by_id_canceled.json").read_bytes()
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


def test_perp_order_by_tx_preserves_exact_values(order_api: SimpleNamespace) -> None:
    """Expose the validated perpetual order and the exact queried association."""
    order = get_deepx_perp_order_by_tx(PERP_HASH, config=order_api.config)

    assert isinstance(order, DeepXPerpOrderRecord)
    assert order.order_id == "1789445193480"
    assert order.owner == "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
    assert order.size == "0.3"
    assert order.price == "2523.683573"
    assert order.avg_fill_price == "2499.6"
    assert order.fee == "-0.149921"
    assert order.tx_hash == PERP_HASH
    assert urlparse(order_api.paths[0]).path == "/internal/v1/account/perp/order-by-tx"
    assert parse_qs(urlparse(order_api.paths[0]).query) == {"txHash": [PERP_HASH]}


def test_spot_order_by_tx_preserves_exact_values(order_api: SimpleNamespace) -> None:
    """Expose the validated Spot order and its cancellation fields."""
    order = get_deepx_spot_order_by_tx(SPOT_HASH, config=order_api.config)

    assert isinstance(order, DeepXSpotOrderRecord)
    assert order.order_id == "1789627379350"
    assert order.maker == "0x08bdb660e03c75954e0fcaa1e7ead150a92c891c"
    assert order.price == "2456.05"
    assert order.base_amount == "0.8849"
    assert order.cancel_reason == "UserCanceled"
    assert order.cancel_height == 188574100
    assert order.tx_hash == SPOT_HASH
    assert urlparse(order_api.paths[0]).path == "/internal/v1/account/spot/order-by-tx"
    assert parse_qs(urlparse(order_api.paths[0]).query) == {"txHash": [SPOT_HASH]}


@pytest.mark.parametrize(
    ("reader", "path", "tx_hash"),
    [
        (get_deepx_perp_order_by_tx, "/internal/v1/account/perp/order-by-tx", PERP_HASH),
        (get_deepx_spot_order_by_tx, "/internal/v1/account/spot/order-by-tx", SPOT_HASH),
    ],
)
def test_order_by_tx_rejects_mismatched_response(
    order_api: SimpleNamespace,
    reader: Callable[..., DeepXPerpOrderRecord | DeepXSpotOrderRecord],
    path: str,
    tx_hash: str,
) -> None:
    """Reject a response associated with another transaction."""
    order_api.responses[path]["data"]["txHash"] = "0x" + "11" * 32

    with pytest.raises(ValueError, match="another transaction hash"):
        reader(tx_hash, config=order_api.config)


@pytest.mark.parametrize("reader", [get_deepx_perp_order_by_tx, get_deepx_spot_order_by_tx])
def test_order_by_tx_rejects_invalid_hash_before_network(
    order_api: SimpleNamespace,
    reader: Callable[..., DeepXPerpOrderRecord | DeepXSpotOrderRecord],
) -> None:
    """Reject a malformed hash without querying the venue."""
    with pytest.raises(ValueError, match="32-byte tx_hash"):
        reader("0x12", config=order_api.config)

    assert order_api.paths == []
