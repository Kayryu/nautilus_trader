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

"""Tests for the read-only DeepX Python quota summary boundary."""

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
from nautilus_trader.adapters.deepx import DeepXQuotaHistoryPage
from nautilus_trader.adapters.deepx import DeepXQuotaHistoryRecord
from nautilus_trader.adapters.deepx import DeepXQuotaSummary
from nautilus_trader.adapters.deepx import get_deepx_quota_history
from nautilus_trader.adapters.deepx import get_deepx_quota_history_pages
from nautilus_trader.adapters.deepx import get_deepx_quota_summary


WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/deepx-rust-sdk/test_data/http/testnet/wallet_quota_summary.json"
)
HISTORY_FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/deepx-rust-sdk/test_data/http/testnet/quota_history_purchase_page.json"
)
PURCHASE_WALLET = "0x0a40c3efbc3b3bebdf1fb6f0f8c612eb336b25ae"
PURCHASE_BUYER = "0x5f5f4a75ca927ff4ef5f2debb6214ee2b5e63eb2"


@pytest.fixture
def quota_api() -> Iterator[SimpleNamespace]:
    """Serve the captured quota response from a local read-only endpoint."""
    response = SimpleNamespace(body=FIXTURE.read_bytes(), bodies=[], paths=[])

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            response.paths.append(self.path)
            body = response.bodies.pop(0) if response.bodies else response.body
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
        network=DeepXNetworkConfig(base_url_rest=f"http://127.0.0.1:{server.server_port}"),
    )
    try:
        yield response
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_quota_summary_exposes_validated_exact_values(quota_api: SimpleNamespace) -> None:
    """Preserve exact volumes and bind the request to the expected endpoint."""
    summary = get_deepx_quota_summary(WALLET, quota_api.config)

    assert isinstance(summary, DeepXQuotaSummary)
    assert summary.owner == WALLET
    assert summary.subaccount_count == 4
    assert summary.spot_volume_usd == "0.000000000000000000"
    assert summary.perp_volume_usd == "2970.527580000000000000"
    assert summary.total_volume_usd == "2970.527580000000000000"
    assert summary.quota_pending == 2970
    assert summary.first_trade_ts_ms == 1789445055647
    assert summary.updated_at == "2026-09-16T01:29:47.985787Z"
    assert len(quota_api.paths) == 1
    path = urlparse(quota_api.paths[0])
    assert path.path == "/internal/v1/account/quota/summary"
    assert parse_qs(path.query) == {"wallet": [WALLET]}


def test_quota_summary_rejects_foreign_owner(quota_api: SimpleNamespace) -> None:
    """Reject a well-formed response for another wallet."""
    payload = json.loads(quota_api.body)
    payload["data"]["owner"] = "0x1111111111111111111111111111111111111111"
    quota_api.body = json.dumps(payload).encode()

    with pytest.raises(ValueError, match="owner does not match"):
        get_deepx_quota_summary(WALLET, quota_api.config)


def test_quota_summary_rejects_invalid_wallet_before_request(quota_api: SimpleNamespace) -> None:
    """Reject malformed wallet addresses without contacting the server."""
    with pytest.raises(ValueError, match="20-byte hex wallet address"):
        get_deepx_quota_summary("invalid", quota_api.config)

    assert quota_api.paths == []


def test_quota_history_preserves_chain_record_and_request_filters(
    quota_api: SimpleNamespace,
) -> None:
    """Expose a validated purchase page with its chain and payer identities."""
    quota_api.body = HISTORY_FIXTURE.read_bytes()

    page = get_deepx_quota_history(
        PURCHASE_WALLET,
        buyer_address=PURCHASE_BUYER,
        history_type="purchase",
        limit=5,
        config=quota_api.config,
    )

    assert isinstance(page, DeepXQuotaHistoryPage)
    assert not page.has_next
    assert page.next_cursor is None
    assert len(page.items) == 1
    record = page.items[0]
    assert isinstance(record, DeepXQuotaHistoryRecord)
    assert record.history_type == "purchase"
    assert record.owner_address == PURCHASE_WALLET
    assert record.buyer_type == "wallet"
    assert record.buyer_address == PURCHASE_BUYER
    assert record.quota == 10000
    assert record.block_number == 181248894
    assert record.event_index == 49
    assert record.tx_hash_type == "EXTRINSIC_HASH"
    assert record.created_at == "2026-09-11T08:16:38.475Z"
    assert len(quota_api.paths) == 1
    path = urlparse(quota_api.paths[0])
    assert path.path == "/internal/v1/account/quota/history"
    assert parse_qs(path.query) == {
        "wallet": [PURCHASE_WALLET],
        "buyerAddress": [PURCHASE_BUYER],
        "historyType": ["purchase"],
        "limit": ["5"],
    }


def test_quota_history_accepts_empty_wallet_page(quota_api: SimpleNamespace) -> None:
    """Return an empty page for a wallet with no confirmed quota operations."""
    quota_api.body = json.dumps(
        {
            "code": 200,
            "msg": "success",
            "data": {"items": [], "nextCursor": None, "hasNext": False},
            "fail": False,
        },
    ).encode()

    page = get_deepx_quota_history(WALLET, config=quota_api.config)

    assert page.items == []
    assert not page.has_next
    assert page.next_cursor is None


def test_quota_history_pages_follow_cursor_and_stop_at_terminal_page(
    quota_api: SimpleNamespace,
) -> None:
    """Collect the validated chain record and a terminal page in request order."""
    first = json.loads(HISTORY_FIXTURE.read_bytes())
    first["data"]["hasNext"] = True
    first["data"]["nextCursor"] = "next-purchase"
    terminal = {
        "code": 200,
        "msg": "success",
        "data": {"items": [], "nextCursor": None, "hasNext": False},
        "fail": False,
    }
    quota_api.bodies = [json.dumps(first).encode(), json.dumps(terminal).encode()]

    pages = get_deepx_quota_history_pages(
        PURCHASE_WALLET,
        2,
        buyer_address=PURCHASE_BUYER,
        history_type="purchase",
        limit=5,
        config=quota_api.config,
    )

    assert len(pages) == 2
    assert pages[0].items[0].quota == 10000
    assert pages[0].next_cursor == "next-purchase"
    assert pages[1].items == []
    assert not pages[1].has_next
    assert len(quota_api.paths) == 2
    assert "cursor" not in parse_qs(urlparse(quota_api.paths[0]).query)
    assert parse_qs(urlparse(quota_api.paths[1]).query)["cursor"] == ["next-purchase"]


def test_quota_history_pages_reject_exhausted_budget_without_partial_result(
    quota_api: SimpleNamespace,
) -> None:
    """Stop before a second HTTP read when the first page exhausts the budget."""
    first = json.loads(HISTORY_FIXTURE.read_bytes())
    first["data"]["hasNext"] = True
    first["data"]["nextCursor"] = "next-purchase"
    quota_api.body = json.dumps(first).encode()

    with pytest.raises(ValueError, match="pagination exceeded its limit of 1 pages"):
        get_deepx_quota_history_pages(PURCHASE_WALLET, 1, config=quota_api.config)

    assert len(quota_api.paths) == 1


def test_quota_history_rejects_foreign_owner(quota_api: SimpleNamespace) -> None:
    """Reject a chain record whose owner differs from the requested wallet."""
    payload = json.loads(HISTORY_FIXTURE.read_bytes())
    payload["data"]["items"][0]["ownerAddress"] = WALLET
    quota_api.body = json.dumps(payload).encode()

    with pytest.raises(ValueError, match="invalid scope or identity"):
        get_deepx_quota_history(PURCHASE_WALLET, config=quota_api.config)


@pytest.mark.parametrize(
    ("history_type", "limit", "message"),
    [
        ("unknown", None, "purchase, activate, or free"),
        (None, 0, "limit must be between 1 and 100"),
        (None, 101, "limit must be between 1 and 100"),
    ],
)
def test_quota_history_rejects_invalid_filters_before_request(
    quota_api: SimpleNamespace,
    history_type: str | None,
    limit: int | None,
    message: str,
) -> None:
    """Reject unsupported classifications and page sizes without a network request."""
    with pytest.raises(ValueError, match=message):
        get_deepx_quota_history(
            WALLET,
            history_type=history_type,
            limit=limit,
            config=quota_api.config,
        )

    assert quota_api.paths == []
