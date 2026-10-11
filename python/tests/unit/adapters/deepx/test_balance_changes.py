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

"""Tests for read-only DeepX Python balance-change pages."""

import json
from collections.abc import Iterator
from copy import deepcopy
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from types import SimpleNamespace
from urllib.parse import parse_qs
from urllib.parse import urlparse

import pytest

from nautilus_trader.adapters.deepx import DeepXBalanceChangePosition
from nautilus_trader.adapters.deepx import DeepXBalanceChangeRecord
from nautilus_trader.adapters.deepx import DeepXBalanceChangesPage
from nautilus_trader.adapters.deepx import DeepXDataClientConfig
from nautilus_trader.adapters.deepx import DeepXNetworkConfig
from nautilus_trader.adapters.deepx import get_deepx_balance_change_pages
from nautilus_trader.adapters.deepx import get_deepx_balance_changes


WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
SUBACCOUNT = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/test_data/http/testnet/wallet_balance_changes_page_2.json"
)


@pytest.fixture
def balance_api() -> Iterator[SimpleNamespace]:
    """Serve a captured balance-change page from a local HTTP endpoint."""
    response = SimpleNamespace(body=FIXTURE.read_bytes(), bodies=None, paths=[])

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            response.paths.append(self.path)
            body = (
                response.body
                if response.bodies is None
                else response.bodies[len(response.paths) - 1]
            )
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


def test_balance_changes_preserve_exact_records_and_cursor(balance_api: SimpleNamespace) -> None:
    """Expose captured signed changes without float conversion or cursor interpretation."""
    page = get_deepx_balance_changes(
        wallet=WALLET,
        start_ms=1789498000000,
        end_ms=1789523000000,
        change_types=["FUNDING_FEE", "SETTLEMENT"],
        page_size=2,
        config=balance_api.config,
    )

    assert isinstance(page, DeepXBalanceChangesPage)
    assert page.has_next
    assert page.next_cursor == "MTc4OTQ5ODIxMTExNToxODY3Mjg3MTcwMDAyMDAw"
    assert len(page.items) == 2
    fee = page.items[0]
    assert isinstance(fee, DeepXBalanceChangeRecord)
    assert fee.id == 1870712120001300
    assert fee.asset == "USDC"
    assert fee.balance_change == "-0.00017"
    assert fee.change_type == "FUNDING_FEE"
    assert fee.time == 1789522187243
    assert fee.height == 187071212
    assert fee.event_idx == 13
    assert fee.tx_hash is None
    assert fee.tx_hash_type is None
    assert fee.from_address is None
    assert fee.to_address is None
    assert fee.cross_chain_json is None
    assert isinstance(fee.position, DeepXBalanceChangePosition)
    assert fee.position.id == 1659167
    assert fee.position.owner == SUBACCOUNT
    assert fee.position.market_id == 3
    settlement = page.items[1]
    assert settlement.balance_change == "-10.235558"
    assert settlement.change_type == "SETTLEMENT"
    assert settlement.tx_hash_type == "EXTRINSIC_HASH"
    assert settlement.position is None
    path = urlparse(balance_api.paths[0])
    assert path.path == "/internal/v1/account/balance-changes"
    assert parse_qs(path.query) == {
        "wallet": [WALLET],
        "startTime": ["1789498000000"],
        "endTime": ["1789523000000"],
        "changeType": ["FUNDING_FEE,SETTLEMENT"],
        "pageSize": ["2"],
    }


def test_balance_changes_accept_subaccount_scope_and_opaque_json(
    balance_api: SimpleNamespace,
) -> None:
    """Keep subaccount scope and uninterpreted cross-chain data explicit."""
    payload = json.loads(balance_api.body)
    payload["data"]["items"][0]["crossChain"] = {"source": "opaque", "sequence": 7}
    balance_api.body = json.dumps(payload).encode()

    page = get_deepx_balance_changes(
        subaccount=SUBACCOUNT,
        cursor="opaque-next",
        config=balance_api.config,
    )

    assert json.loads(page.items[0].cross_chain_json) == {"source": "opaque", "sequence": 7}
    assert parse_qs(urlparse(balance_api.paths[0]).query) == {
        "user": [SUBACCOUNT],
        "cursor": ["opaque-next"],
    }


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({}, "exactly one subaccount or wallet"),
        ({"wallet": WALLET, "subaccount": SUBACCOUNT}, "exactly one subaccount or wallet"),
        ({"wallet": "invalid"}, "20-byte hex account address"),
        ({"wallet": WALLET, "start_ms": 2, "end_ms": 1}, "start_ms must not exceed end_ms"),
        ({"wallet": WALLET, "page_size": 0}, "page_size must be greater than zero"),
        ({"wallet": WALLET, "cursor": ""}, "cursor must not be empty"),
        ({"wallet": WALLET, "change_types": ["OTHER"]}, "unknown balance change type"),
        (
            {"wallet": WALLET, "change_types": ["FUNDING_FEE", "FUNDING_FEE"]},
            "duplicate change types",
        ),
    ],
)
def test_balance_changes_reject_invalid_request_before_transport(
    balance_api: SimpleNamespace,
    kwargs: dict[str, object],
    message: str,
) -> None:
    """Reject invalid scope and filters without contacting the venue."""
    with pytest.raises(ValueError, match=message):
        get_deepx_balance_changes(**kwargs, config=balance_api.config)

    assert balance_api.paths == []


def test_balance_changes_reject_foreign_position(balance_api: SimpleNamespace) -> None:
    """Reject a position owned by another subaccount in subaccount scope."""
    with pytest.raises(ValueError, match="foreign position identity"):
        get_deepx_balance_changes(
            subaccount="0x1111111111111111111111111111111111111111",
            config=balance_api.config,
        )


def two_balance_change_pages(body: bytes) -> list[dict]:
    """Split the captured wallet records across one cursor continuation."""
    response = json.loads(body)
    first = deepcopy(response)
    second = deepcopy(response)
    first["data"]["items"] = [deepcopy(response["data"]["items"][0])]
    second["data"]["items"] = [deepcopy(response["data"]["items"][1])]
    second["data"]["nextCursor"] = None
    second["data"]["hasNext"] = False
    return [first, second]


def test_balance_change_pages_collect_exact_wallet_history(balance_api: SimpleNamespace) -> None:
    """Collect two validated wallet pages and forward the opaque cursor."""
    pages = two_balance_change_pages(balance_api.body)
    balance_api.bodies = [json.dumps(page).encode() for page in pages]

    collected = get_deepx_balance_change_pages(
        max_pages=2,
        wallet=WALLET,
        page_size=1,
        config=balance_api.config,
    )

    assert [page.items[0].id for page in collected] == [1870712120001300, 1867287170002000]
    assert collected[0].items[0].balance_change == "-0.00017"
    assert collected[1].items[0].balance_change == "-10.235558"
    assert collected[0].has_next
    assert not collected[1].has_next
    assert [parse_qs(urlparse(path).query).get("cursor") for path in balance_api.paths] == [
        None,
        [pages[0]["data"]["nextCursor"]],
    ]


def test_balance_change_pages_reject_cross_page_duplicate(balance_api: SimpleNamespace) -> None:
    """Reject a repeated record identity across pages without a partial result."""
    pages = two_balance_change_pages(balance_api.body)
    pages[1]["data"]["items"][0]["id"] = pages[0]["data"]["items"][0]["id"]
    balance_api.bodies = [json.dumps(page).encode() for page in pages]

    with pytest.raises(ValueError, match="repeats record ID"):
        get_deepx_balance_change_pages(max_pages=2, wallet=WALLET, config=balance_api.config)

    assert len(balance_api.paths) == 2


def test_balance_change_pages_stop_on_invalid_first_page(balance_api: SimpleNamespace) -> None:
    """Do not fetch the continuation after an invalid signed amount."""
    pages = two_balance_change_pages(balance_api.body)
    pages[0]["data"]["items"][0]["balanceChange"] = "invalid"
    balance_api.bodies = [json.dumps(page).encode() for page in pages]

    with pytest.raises(ValueError, match="balance changes"):
        get_deepx_balance_change_pages(max_pages=2, wallet=WALLET, config=balance_api.config)

    assert len(balance_api.paths) == 1


def test_balance_change_pages_reject_exhausted_budget(balance_api: SimpleNamespace) -> None:
    """Reject a nonterminal first page at the caller's page limit."""
    pages = two_balance_change_pages(balance_api.body)
    balance_api.bodies = [json.dumps(page).encode() for page in pages]

    with pytest.raises(ValueError, match="exceeded its limit of 1 pages"):
        get_deepx_balance_change_pages(max_pages=1, wallet=WALLET, config=balance_api.config)

    assert len(balance_api.paths) == 1


def test_balance_change_pages_reject_zero_budget(balance_api: SimpleNamespace) -> None:
    """Reject an invalid page budget before contacting the endpoint."""
    with pytest.raises(ValueError, match="max_pages must be greater than zero"):
        get_deepx_balance_change_pages(max_pages=0, wallet=WALLET, config=balance_api.config)

    assert balance_api.paths == []
