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

"""Tests for one-shot DeepX transaction-status observations."""

import json
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from threading import Event
from threading import Thread
from types import SimpleNamespace
from urllib.parse import parse_qs
from urllib.parse import urlparse

import pytest

from nautilus_trader.adapters.deepx import DeepXDataClientConfig
from nautilus_trader.adapters.deepx import DeepXNetworkConfig
from nautilus_trader.adapters.deepx import DeepXRestStatusPoller
from nautilus_trader.adapters.deepx import DeepXRestStatusPollResult
from nautilus_trader.adapters.deepx import DeepXRestTransactionStatus
from nautilus_trader.adapters.deepx import get_deepx_rest_transaction_status


HASH = "0x" + "ab" * 32
OTHER_HASH = "0x" + "cd" * 32


@pytest.fixture
def status_api() -> Iterator[SimpleNamespace]:
    """Serve transaction-status responses from loopback."""
    state = SimpleNamespace(
        body=json.dumps(
            {
                "code": 200,
                "msg": "success",
                "fail": False,
                "data": {
                    "tx_hash": HASH,
                    "confirmation": "best",
                    "order_id": "18446744073709551615",
                    "status": "included",
                    "backend_counter": 18446744073709551615,
                },
            }
        ),
        paths=[],
        bodies=None,
        statuses=None,
        wait_on_second=False,
        second_entered=Event(),
        release_second=Event(),
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.paths.append(self.path)
            attempt = len(state.paths) - 1
            if state.wait_on_second and attempt == 1:
                state.second_entered.set()
                state.release_second.wait(timeout=5)
                return
            body = (state.bodies[attempt] if state.bodies is not None else state.body).encode()
            self.send_response(state.statuses[attempt] if state.statuses is not None else 200)
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
        state.release_second.set()
        server.shutdown()
        server.server_close()
        thread.join()


def test_transaction_status_preserves_backend_evidence(status_api: SimpleNamespace) -> None:
    """Expose a hash-bound status and retain uninterpreted JSON lexemes."""
    result = get_deepx_rest_transaction_status(HASH, config=status_api.config)

    assert isinstance(result, DeepXRestTransactionStatus)
    assert result.tx_hash == HASH
    assert result.confirmation == "best"
    assert result.order_id == 18446744073709551615
    assert result.backend_status == "included"
    assert '"backend_counter": 18446744073709551615' in result.data_json
    assert urlparse(status_api.paths[0]).path == "/internal/v1/chain/tx/status"
    assert parse_qs(urlparse(status_api.paths[0]).query) == {"txHash": [HASH]}


def test_transaction_status_keeps_pending_without_order_id(status_api: SimpleNamespace) -> None:
    """Keep pending separate from an order result."""
    data = json.loads(status_api.body)
    data["data"].update(confirmation="pending", order_id="", status="queued")
    status_api.body = json.dumps(data)

    result = get_deepx_rest_transaction_status(HASH, config=status_api.config)

    assert result.confirmation == "pending"
    assert result.order_id is None
    assert result.backend_status == "queued"


def test_transaction_status_rejects_mismatched_hash(status_api: SimpleNamespace) -> None:
    """Reject a backend observation for a different transaction."""
    data = json.loads(status_api.body)
    data["data"]["tx_hash"] = OTHER_HASH
    status_api.body = json.dumps(data)

    with pytest.raises(ValueError, match="transaction hash mismatch"):
        get_deepx_rest_transaction_status(HASH, config=status_api.config)


def test_transaction_status_keeps_not_found_as_error(status_api: SimpleNamespace) -> None:
    """Do not interpret backend not-found as non-inclusion."""
    status_api.body = json.dumps(
        {"code": 10020, "msg": "Resource not found.", "data": None, "fail": True}
    )

    with pytest.raises(ValueError, match="10020"):
        get_deepx_rest_transaction_status(HASH, config=status_api.config)


def test_transaction_status_rejects_invalid_hash_before_network(
    status_api: SimpleNamespace,
) -> None:
    """Reject a malformed transaction hash before any GET."""
    with pytest.raises(ValueError, match="0x-prefixed 32-byte hash"):
        get_deepx_rest_transaction_status("0x12", config=status_api.config)

    assert status_api.paths == []


def test_status_poller_stops_on_best_and_keeps_last_observation(
    status_api: SimpleNamespace,
) -> None:
    """Follow pending once and expose the terminal backend observation."""
    pending = json.loads(status_api.body)
    pending["data"].update(confirmation="pending", order_id="")
    status_api.bodies = [json.dumps(pending), status_api.body]
    poller = DeepXRestStatusPoller(status_api.config)

    result = poller.poll(HASH, max_attempts=3, interval_ms=1, timeout_ms=1000)

    assert result.termination == "best"
    assert result.attempts == 2
    assert result.last_observation.order_id == 18446744073709551615
    assert result.last_error is None
    assert len(status_api.paths) == 2


def test_status_poller_exhausts_pending_budget(status_api: SimpleNamespace) -> None:
    """Retain pending status without inventing a terminal result."""
    pending = json.loads(status_api.body)
    pending["data"].update(confirmation="pending", order_id="")
    status_api.body = json.dumps(pending)
    poller = DeepXRestStatusPoller(status_api.config)

    result = poller.poll(HASH, max_attempts=2, interval_ms=1, timeout_ms=1000)

    assert result.termination == "attempt_limit"
    assert result.attempts == 2
    assert result.last_observation.confirmation == "pending"
    assert result.last_error is None
    assert len(status_api.paths) == 2


def test_status_poller_retains_pending_after_retryable_error(status_api: SimpleNamespace) -> None:
    """Preserve the last valid observation and the later query failure."""
    pending = json.loads(status_api.body)
    pending["data"].update(confirmation="pending", order_id="")
    status_api.bodies = [json.dumps(pending), "unavailable"]
    status_api.statuses = [200, 503]
    poller = DeepXRestStatusPoller(status_api.config)

    result = poller.poll(HASH, max_attempts=2, interval_ms=1, timeout_ms=1000)

    assert result.termination == "attempt_limit"
    assert result.attempts == 2
    assert result.last_observation.confirmation == "pending"
    assert "503" in result.last_error
    assert len(status_api.paths) == 2


def test_status_poller_stops_on_not_found(status_api: SimpleNamespace) -> None:
    """Do not retry or reinterpret backend not-found."""
    status_api.body = json.dumps(
        {"code": 10020, "msg": "Resource not found.", "data": None, "fail": True}
    )
    poller = DeepXRestStatusPoller(status_api.config)

    result = poller.poll(HASH, max_attempts=3, interval_ms=1, timeout_ms=1000)

    assert result.termination == "failed"
    assert result.attempts == 1
    assert result.last_observation is None
    assert "10020" in result.last_error
    assert len(status_api.paths) == 1


def test_status_poller_rejects_invalid_limits_before_network(status_api: SimpleNamespace) -> None:
    """Validate explicit attempt and timing bounds before querying."""
    poller = DeepXRestStatusPoller(status_api.config)

    with pytest.raises(ValueError, match="max_attempts must be greater than zero"):
        poller.poll(HASH, max_attempts=0, interval_ms=1, timeout_ms=1000)
    with pytest.raises(ValueError, match="interval_ms and timeout_ms"):
        poller.poll(HASH, max_attempts=2, interval_ms=0, timeout_ms=1000)
    with pytest.raises(ValueError, match="0x-prefixed 32-byte hash"):
        poller.poll("0x12", max_attempts=2, interval_ms=1, timeout_ms=1000)

    assert status_api.paths == []


def test_status_poller_can_cancel_an_in_flight_request(status_api: SimpleNamespace) -> None:
    """Cancel the second GET from another Python thread and retain pending."""
    pending = json.loads(status_api.body)
    pending["data"].update(confirmation="pending", order_id="")
    status_api.body = json.dumps(pending)
    status_api.wait_on_second = True
    poller = DeepXRestStatusPoller(status_api.config)
    outcome: list[DeepXRestStatusPollResult] = []

    def poll() -> None:
        outcome.append(poller.poll(HASH, max_attempts=3, interval_ms=1, timeout_ms=3000))

    thread = Thread(target=poll, daemon=True)
    thread.start()
    assert status_api.second_entered.wait(timeout=2)
    poller.cancel()
    thread.join(timeout=2)
    status_api.release_second.set()

    assert not thread.is_alive()
    assert poller.cancelled
    assert len(outcome) == 1
    result = outcome[0]
    assert result.termination == "cancelled"
    assert result.attempts == 2
    assert result.last_observation.confirmation == "pending"
    assert result.last_error is None
