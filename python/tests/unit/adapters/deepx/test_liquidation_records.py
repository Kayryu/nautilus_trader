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

"""Tests for read-only DeepX Python liquidation history."""

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

from nautilus_trader.adapters.deepx import DeepXDataClientConfig
from nautilus_trader.adapters.deepx import DeepXLiquidationRecord
from nautilus_trader.adapters.deepx import DeepXLiquidationRecordsPage
from nautilus_trader.adapters.deepx import DeepXNetworkConfig
from nautilus_trader.adapters.deepx import get_deepx_liquidation_record_pages
from nautilus_trader.adapters.deepx import get_deepx_liquidation_records


WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
FIXTURE = (
    Path(__file__).resolve().parents[5]
    / "crates/adapters/deepx/test_data/http/testnet/wallet_liquidation_records_page_1.json"
)


@pytest.fixture
def liquidation_api() -> Iterator[SimpleNamespace]:
    """Serve the captured wallet liquidation response from a local endpoint."""
    state = SimpleNamespace(response=json.loads(FIXTURE.read_bytes()), responses=None, paths=[])

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.paths.append(self.path)
            response = (
                state.response if state.responses is None else state.responses[len(state.paths) - 1]
            )
            body = json.dumps(response).encode()
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


def two_liquidation_pages(response: dict) -> list[dict]:
    """Split the captured ordered records into two cursor-linked pages."""
    first = deepcopy(response)
    second = deepcopy(response)
    first["data"]["items"] = deepcopy(response["data"]["items"][:3])
    second["data"]["items"] = deepcopy(response["data"]["items"][3:])
    second["data"]["nextCursor"] = None
    second["data"]["hasNext"] = False
    return [first, second]


def test_liquidation_records_preserve_raw_units_and_json(
    liquidation_api: SimpleNamespace,
) -> None:
    """Expose exact raw amounts and uninterpreted detail JSON."""
    page = get_deepx_liquidation_records(
        wallet=WALLET,
        liquidation_types=["LiquidatePerp"],
        page_size=5,
        config=liquidation_api.config,
    )

    assert isinstance(page, DeepXLiquidationRecordsPage)
    assert page.has_next
    assert len(page.items) == 5
    record = page.items[0]
    assert isinstance(record, DeepXLiquidationRecord)
    assert record.id == 1763
    assert record.liquidation_id == 2
    assert record.liquidation_type == "LiquidatePerp"
    assert record.target_account == "0x7af9d31794004cc061b239ac098e998e47095de1"
    assert record.margin_shortage == "2771"
    assert record.margin_freed == "16917"
    assert record.liquidator_fee == "16911"
    assert record.if_fee == "0"
    assert record.liquidate_base_amount == "20000000"
    assert record.oracle_price == "84566331"
    assert record.liquidator_order_id == 0
    assert record.target_account_order_id == 14
    assert record.borrow_amount is None
    assert not record.bankrupt
    assert json.loads(record.liquidation_detail_json)["liquidatePerp"]["market_index"] == 4
    assert json.loads(record.canceled_order_ids_json) == {
        "perpOrderIds": [],
        "spotOrderIds": [],
    }
    assert parse_qs(urlparse(liquidation_api.paths[0]).query) == {
        "wallet": [WALLET],
        "liquidationType": ["LiquidatePerp"],
        "sort": ["DESC"],
        "pageSize": ["5"],
    }


def test_liquidation_records_preserve_u128_max(liquidation_api: SimpleNamespace) -> None:
    """Expose a full-range raw integer without floating-point conversion."""
    maximum = str(2**128 - 1)
    liquidation_api.response["data"]["items"][0]["marginShortage"] = maximum

    page = get_deepx_liquidation_records(wallet=WALLET, config=liquidation_api.config)

    assert page.items[0].margin_shortage == maximum


def test_liquidation_record_pages_follow_cursor(liquidation_api: SimpleNamespace) -> None:
    """Collect both pages in requested descending time order."""
    liquidation_api.responses = two_liquidation_pages(liquidation_api.response)

    pages = get_deepx_liquidation_record_pages(
        max_pages=2,
        wallet=WALLET,
        page_size=3,
        config=liquidation_api.config,
    )

    assert [record.id for page in pages for record in page.items] == [1763, 1756, 1750, 1748, 1754]
    assert pages[0].has_next
    assert not pages[1].has_next
    assert [parse_qs(urlparse(path).query).get("cursor") for path in liquidation_api.paths] == [
        None,
        [liquidation_api.responses[0]["data"]["nextCursor"]],
    ]


def test_liquidation_record_pages_reject_duplicate_id(liquidation_api: SimpleNamespace) -> None:
    """Reject a repeated record ID across pages without a partial result."""
    liquidation_api.responses = two_liquidation_pages(liquidation_api.response)
    liquidation_api.responses[1]["data"]["items"][0]["id"] = 1763

    with pytest.raises(ValueError, match="repeats record ID"):
        get_deepx_liquidation_record_pages(
            max_pages=2,
            wallet=WALLET,
            page_size=3,
            config=liquidation_api.config,
        )

    assert len(liquidation_api.paths) == 2


def test_liquidation_record_pages_stop_on_invalid_first_page(
    liquidation_api: SimpleNamespace,
) -> None:
    """Do not request a continuation after invalid raw financial data."""
    liquidation_api.responses = two_liquidation_pages(liquidation_api.response)
    liquidation_api.responses[0]["data"]["items"][0]["marginShortage"] = "-1"

    with pytest.raises(ValueError, match="item 0 failed decoding"):
        get_deepx_liquidation_record_pages(
            max_pages=2,
            wallet=WALLET,
            page_size=3,
            config=liquidation_api.config,
        )

    assert len(liquidation_api.paths) == 1


def test_liquidation_record_pages_reject_exhausted_budget(
    liquidation_api: SimpleNamespace,
) -> None:
    """Stop at the budget when the first page has a continuation."""
    liquidation_api.responses = two_liquidation_pages(liquidation_api.response)

    with pytest.raises(ValueError, match="exceeded its limit of 1 pages"):
        get_deepx_liquidation_record_pages(
            max_pages=1,
            wallet=WALLET,
            page_size=3,
            config=liquidation_api.config,
        )

    assert len(liquidation_api.paths) == 1


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({}, "exactly one subaccount or wallet"),
        ({"wallet": WALLET, "sort": "NEWEST"}, "sort must be ASC or DESC"),
        ({"wallet": WALLET, "liquidation_types": ["Other"]}, "unknown liquidation type"),
        (
            {"wallet": WALLET, "liquidation_types": ["LiquidatePerp", "LiquidatePerp"]},
            "duplicate liquidation types",
        ),
    ],
)
def test_liquidation_records_reject_invalid_request_before_transport(
    liquidation_api: SimpleNamespace,
    kwargs: dict[str, object],
    message: str,
) -> None:
    """Validate scope, sort, and type filters before sending HTTP."""
    with pytest.raises(ValueError, match=message):
        get_deepx_liquidation_records(**kwargs, config=liquidation_api.config)

    assert liquidation_api.paths == []


def test_liquidation_record_pages_reject_zero_budget(liquidation_api: SimpleNamespace) -> None:
    """Reject a zero page budget before contacting the venue."""
    with pytest.raises(ValueError, match="max_pages must be greater than zero"):
        get_deepx_liquidation_record_pages(
            max_pages=0,
            wallet=WALLET,
            config=liquidation_api.config,
        )

    assert liquidation_api.paths == []
