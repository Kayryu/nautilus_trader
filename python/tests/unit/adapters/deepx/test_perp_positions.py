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

"""Tests for read-only DeepX perpetual position lifecycle pages."""

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
from nautilus_trader.adapters.deepx import DeepXNetworkConfig
from nautilus_trader.adapters.deepx import DeepXPerpPositionRecord
from nautilus_trader.adapters.deepx import DeepXPerpPositionsPage
from nautilus_trader.adapters.deepx import DeepXSubaccountPerpPositions
from nautilus_trader.adapters.deepx import DeepXWalletPerpPositions
from nautilus_trader.adapters.deepx import get_deepx_perp_position_pages
from nautilus_trader.adapters.deepx import get_deepx_perp_positions
from nautilus_trader.adapters.deepx import get_deepx_wallet_perp_positions


SUBACCOUNT = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
SECOND_SUBACCOUNT = "0x977654069311fa41b88f7901dc65fae9a76fd60b"
WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
FIXTURE_DIR = Path(__file__).resolve().parents[5] / "crates/adapters/deepx/test_data/http/testnet"
FIXTURE = FIXTURE_DIR / "perp_positions_account_market_3.json"


@pytest.fixture
def positions_api() -> Iterator[SimpleNamespace]:
    """Serve the captured nonempty position page from a local endpoint."""
    state = SimpleNamespace(
        response=json.loads(FIXTURE.read_bytes()),
        responses=None,
        account_responses={},
        final_directory=None,
        position_calls=0,
        directory_calls=0,
        paths=[],
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.paths.append(self.path)
            path = urlparse(self.path).path
            if path == "/internal/v1/account/position":
                response = (
                    state.response
                    if state.responses is None
                    else state.responses[state.position_calls]
                )
                state.position_calls += 1
            elif path == "/internal/v1/account/subaccounts" and state.final_directory is not None:
                response = (
                    state.account_responses[path]
                    if state.directory_calls == 0
                    else state.final_directory
                )
                state.directory_calls += 1
            elif path == "/internal/v1/account/subaccount-info":
                response = deepcopy(state.account_responses[path])
                response["data"]["address"] = parse_qs(urlparse(self.path).query)["address"][0]
            else:
                response = state.account_responses[path]
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


def test_perp_positions_preserve_exact_lifecycle_values(positions_api: SimpleNamespace) -> None:
    """Expose both captured lifecycle records without losing financial digits."""
    page = get_deepx_perp_positions(
        SUBACCOUNT,
        market_id=3,
        page_size=20,
        config=positions_api.config,
    )

    assert isinstance(page, DeepXPerpPositionsPage)
    assert not page.has_next
    assert page.next_cursor is None
    assert len(page.items) == 2
    active, closed = page.items
    assert isinstance(active, DeepXPerpPositionRecord)
    assert active.id == 1659167
    assert active.owner == SUBACCOUNT
    assert active.market_id == 3
    assert active.is_long
    assert active.base_asset_amount == "0.3"
    assert active.entry_price == "2499.6"
    assert active.last_funding_rate == "0.07552088619142279"
    assert active.pnl == "-0.4018472999999176"
    assert active.status == "Open"
    assert active.close_price is None
    assert active.liquidate_price is None
    assert closed.id == 1659157
    assert closed.status == "Closed"
    assert closed.close_price == "2499.41"
    assert closed.close_block_num == 185971191
    assert closed.close_event_idx == 7
    assert [urlparse(path).path for path in positions_api.paths] == [
        "/internal/v1/account/position",
    ]
    assert parse_qs(urlparse(positions_api.paths[0]).query) == {
        "user": [SUBACCOUNT],
        "marketId": ["3"],
        "addressType": ["subaccount"],
        "pageSize": ["20"],
    }


def test_perp_positions_reject_foreign_owner(positions_api: SimpleNamespace) -> None:
    """Reject the whole page when one record belongs to another subaccount."""
    positions_api.response["data"]["items"][1]["owner"] = (
        "0x1111111111111111111111111111111111111111"
    )

    with pytest.raises(ValueError, match="another subaccount"):
        get_deepx_perp_positions(SUBACCOUNT, market_id=3, config=positions_api.config)


def test_perp_positions_reject_invalid_page_size_before_request(
    positions_api: SimpleNamespace,
) -> None:
    """Apply the Rust request bound before contacting the venue."""
    with pytest.raises(ValueError, match="page_size must be greater than zero"):
        get_deepx_perp_positions(SUBACCOUNT, page_size=0, config=positions_api.config)

    assert positions_api.paths == []


def two_position_pages(response: dict) -> list[dict]:
    """Split the captured lifecycle records into two cursor-linked pages."""
    first = deepcopy(response)
    second = deepcopy(response)
    first["data"]["items"] = [response["data"]["items"][0]]
    first["data"]["hasNext"] = True
    first["data"]["nextCursor"] = "next-page"
    second["data"]["items"] = [response["data"]["items"][1]]
    return [first, second]


def configure_wallet_positions(positions_api: SimpleNamespace) -> None:
    """Serve one captured wallet member and its position lifecycle pages."""
    directory = json.loads((FIXTURE_DIR / "wallet_subaccounts.json").read_bytes())
    directory["data"] = [SUBACCOUNT]
    stats = json.loads((FIXTURE_DIR / "wallet_user_stats.json").read_bytes())
    stats["data"]["subaccounts"] = [SUBACCOUNT]
    stats["data"]["numberOfSubAccounts"] = 1
    positions_api.account_responses = {
        "/internal/v1/account/subaccounts": directory,
        "/internal/v1/account/user-stats": stats,
        "/internal/v1/account/subaccount-info": json.loads(
            (FIXTURE_DIR / "subaccount_info.json").read_bytes(),
        ),
    }
    positions_api.responses = two_position_pages(positions_api.response)


def test_perp_position_pages_collect_two_pages(positions_api: SimpleNamespace) -> None:
    """Collect both pages and forward the venue cursor."""
    positions_api.responses = two_position_pages(positions_api.response)

    pages = get_deepx_perp_position_pages(
        SUBACCOUNT,
        2,
        market_id=3,
        page_size=1,
        config=positions_api.config,
    )

    assert [page.items[0].id for page in pages] == [1659167, 1659157]
    assert pages[0].has_next
    assert pages[0].next_cursor == "next-page"
    assert not pages[1].has_next
    assert [parse_qs(urlparse(path).query).get("cursor") for path in positions_api.paths] == [
        None,
        ["next-page"],
    ]


def test_perp_position_pages_reject_cross_page_duplicate(positions_api: SimpleNamespace) -> None:
    """Reject a repeated position ID without returning the first page."""
    positions_api.responses = two_position_pages(positions_api.response)
    positions_api.responses[1]["data"]["items"] = positions_api.responses[0]["data"]["items"]

    with pytest.raises(ValueError, match="repeats position ID"):
        get_deepx_perp_position_pages(SUBACCOUNT, 2, market_id=3, config=positions_api.config)

    assert len(positions_api.paths) == 2


def test_perp_position_pages_stop_on_invalid_first_page(positions_api: SimpleNamespace) -> None:
    """Do not fetch a continuation after a malformed first lifecycle record."""
    positions_api.responses = two_position_pages(positions_api.response)
    positions_api.responses[0]["data"]["items"][0]["owner"] = SECOND_SUBACCOUNT

    with pytest.raises(ValueError, match="another subaccount"):
        get_deepx_perp_position_pages(SUBACCOUNT, 2, market_id=3, config=positions_api.config)

    assert positions_api.position_calls == 1


def test_perp_position_pages_reject_exhausted_budget(positions_api: SimpleNamespace) -> None:
    """Stop at the page budget when the first page has a continuation."""
    positions_api.responses = two_position_pages(positions_api.response)

    with pytest.raises(ValueError, match="exceeded its limit of 1 pages"):
        get_deepx_perp_position_pages(SUBACCOUNT, 1, market_id=3, config=positions_api.config)

    assert len(positions_api.paths) == 1


def test_perp_position_pages_reject_zero_budget(positions_api: SimpleNamespace) -> None:
    """Reject a missing page budget before contacting the venue."""
    with pytest.raises(ValueError, match="max_pages must be greater than zero"):
        get_deepx_perp_position_pages(SUBACCOUNT, 0, config=positions_api.config)

    assert positions_api.paths == []


def test_wallet_perp_positions_collect_validated_directory(positions_api: SimpleNamespace) -> None:
    """Return all pages only after ownership and final directory checks pass."""
    configure_wallet_positions(positions_api)

    result = get_deepx_wallet_perp_positions(
        WALLET,
        max_subaccounts=1,
        max_pages_per_subaccount=2,
        market_id=3,
        page_size=1,
        config=positions_api.config,
    )

    assert isinstance(result, DeepXWalletPerpPositions)
    assert result.directory.wallet == WALLET
    assert result.directory.addresses == [SUBACCOUNT]
    assert result.stats.subaccounts == [SUBACCOUNT]
    assert len(result.subaccounts) == 1
    member = result.subaccounts[0]
    assert isinstance(member, DeepXSubaccountPerpPositions)
    assert member.profile.authority == WALLET
    assert member.profile.address == SUBACCOUNT
    assert [page.items[0].id for page in member.pages] == [1659167, 1659157]
    assert [urlparse(path).path for path in positions_api.paths] == [
        "/internal/v1/account/subaccounts",
        "/internal/v1/account/user-stats",
        "/internal/v1/account/subaccount-info",
        "/internal/v1/account/position",
        "/internal/v1/account/position",
        "/internal/v1/account/subaccounts",
    ]
    assert parse_qs(urlparse(positions_api.paths[4]).query)["cursor"] == ["next-page"]


def test_wallet_perp_positions_reject_foreign_profile(positions_api: SimpleNamespace) -> None:
    """Stop before position reads if the profile has another authority."""
    configure_wallet_positions(positions_api)
    positions_api.account_responses["/internal/v1/account/subaccount-info"]["data"]["authority"] = (
        SECOND_SUBACCOUNT
    )

    with pytest.raises(ValueError, match="authority does not match"):
        get_deepx_wallet_perp_positions(WALLET, 1, 2, config=positions_api.config)

    assert positions_api.position_calls == 0


def test_wallet_perp_positions_reject_directory_stats_mismatch(
    positions_api: SimpleNamespace,
) -> None:
    """Stop before profile and position reads when wallet directories disagree."""
    configure_wallet_positions(positions_api)
    positions_api.account_responses["/internal/v1/account/user-stats"]["data"]["subaccounts"] = [
        SECOND_SUBACCOUNT
    ]

    with pytest.raises(ValueError, match="user stats subaccounts differ from wallet directory"):
        get_deepx_wallet_perp_positions(WALLET, 1, 2, config=positions_api.config)

    assert len(positions_api.paths) == 2


def test_wallet_perp_positions_reject_directory_over_limit(
    positions_api: SimpleNamespace,
) -> None:
    """Reject a larger directory without returning a truncated wallet result."""
    configure_wallet_positions(positions_api)
    positions_api.account_responses["/internal/v1/account/subaccounts"]["data"] = [
        SUBACCOUNT,
        SECOND_SUBACCOUNT,
    ]
    stats = positions_api.account_responses["/internal/v1/account/user-stats"]["data"]
    stats["subaccounts"] = [SUBACCOUNT, SECOND_SUBACCOUNT]
    stats["numberOfSubAccounts"] = 2

    with pytest.raises(ValueError, match="exceeds max_subaccounts limit of 1"):
        get_deepx_wallet_perp_positions(WALLET, 1, 2, config=positions_api.config)

    assert positions_api.position_calls == 0


def test_wallet_perp_positions_reject_cross_subaccount_duplicate(
    positions_api: SimpleNamespace,
) -> None:
    """Reject a repeated lifecycle ID across distinct wallet members."""
    configure_wallet_positions(positions_api)
    positions_api.account_responses["/internal/v1/account/subaccounts"]["data"] = [
        SUBACCOUNT,
        SECOND_SUBACCOUNT,
    ]
    stats = positions_api.account_responses["/internal/v1/account/user-stats"]["data"]
    stats["subaccounts"] = [SUBACCOUNT, SECOND_SUBACCOUNT]
    stats["numberOfSubAccounts"] = 2
    positions_api.responses = [deepcopy(positions_api.response), deepcopy(positions_api.response)]
    positions_api.responses[0]["data"]["items"] = [
        deepcopy(positions_api.response["data"]["items"][0]),
    ]
    positions_api.responses[1]["data"]["items"] = [
        deepcopy(positions_api.response["data"]["items"][0]),
    ]
    positions_api.responses[1]["data"]["items"][0]["owner"] = SECOND_SUBACCOUNT

    with pytest.raises(ValueError, match="repeats across subaccounts"):
        get_deepx_wallet_perp_positions(WALLET, 2, 1, market_id=3, config=positions_api.config)

    assert positions_api.position_calls == 2
    assert positions_api.directory_calls == 0


def test_wallet_perp_positions_reject_changed_directory(positions_api: SimpleNamespace) -> None:
    """Reject a changed wallet membership after collecting position pages."""
    configure_wallet_positions(positions_api)
    positions_api.final_directory = deepcopy(
        positions_api.account_responses["/internal/v1/account/subaccounts"],
    )
    positions_api.final_directory["data"] = [SECOND_SUBACCOUNT]

    with pytest.raises(ValueError, match="directory changed during position collection"):
        get_deepx_wallet_perp_positions(WALLET, 1, 2, market_id=3, config=positions_api.config)

    assert positions_api.directory_calls == 2


@pytest.mark.parametrize(
    ("max_subaccounts", "max_pages", "message"),
    [
        (0, 1, "max_subaccounts must be greater than zero"),
        (1, 0, "max_pages must be greater than zero"),
    ],
)
def test_wallet_perp_positions_reject_zero_limits_before_transport(
    positions_api: SimpleNamespace,
    max_subaccounts: int,
    max_pages: int,
    message: str,
) -> None:
    """Reject unbounded wallet requests before sending any HTTP call."""
    with pytest.raises(ValueError, match=message):
        get_deepx_wallet_perp_positions(
            WALLET,
            max_subaccounts,
            max_pages,
            config=positions_api.config,
        )

    assert positions_api.paths == []


def test_wallet_perp_positions_reject_invalid_wallet_before_transport(
    positions_api: SimpleNamespace,
) -> None:
    """Validate wallet identity before requesting its directory."""
    with pytest.raises(ValueError, match="20-byte hex wallet address"):
        get_deepx_wallet_perp_positions("invalid", 1, 1, config=positions_api.config)

    assert positions_api.paths == []
