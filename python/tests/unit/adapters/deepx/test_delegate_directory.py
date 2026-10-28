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

"""Tests for read-only DeepX Python delegate directory queries."""

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
from nautilus_trader.adapters.deepx import DeepXDelegateAccount
from nautilus_trader.adapters.deepx import DeepXDelegateWallets
from nautilus_trader.adapters.deepx import DeepXNetworkConfig
from nautilus_trader.adapters.deepx import DeepXWalletDelegateAccounts
from nautilus_trader.adapters.deepx import get_deepx_delegate_accounts
from nautilus_trader.adapters.deepx import get_deepx_delegator_accounts


WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
DELEGATE = "0x1b856b9bf1d0ceeb0927a081c759813cd703a54f"
FIXTURE_DIR = Path(__file__).resolve().parents[5] / "crates/adapters/deepx/deepx-rust-sdk/test_data/http/testnet"


@pytest.fixture
def delegate_api() -> Iterator[SimpleNamespace]:
    """Serve captured forward and reverse directories from a local HTTP endpoint."""
    response = SimpleNamespace(
        forward=json.loads((FIXTURE_DIR / "wallet_delegate_accounts.json").read_bytes()),
        reverse=json.loads((FIXTURE_DIR / "delegate_delegator_accounts.json").read_bytes()),
        paths=[],
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            response.paths.append(self.path)
            path = urlparse(self.path).path
            payload = response.forward if path.endswith("/delegate-accounts") else response.reverse
            body = json.dumps(payload).encode()
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


def test_forward_and_reverse_delegate_directory(delegate_api: SimpleNamespace) -> None:
    """Expose the validated request scope and captured delegate fields."""
    forward = get_deepx_delegate_accounts(WALLET, delegate_api.config)
    reverse = get_deepx_delegator_accounts(DELEGATE, delegate_api.config)

    assert isinstance(forward, DeepXWalletDelegateAccounts)
    assert forward.wallet == WALLET
    assert len(forward.accounts) == 1
    account = forward.accounts[0]
    assert isinstance(account, DeepXDelegateAccount)
    assert account.delegate_address == DELEGATE
    assert account.delegate_name == "One-Click Trading"
    assert account.mode == "PlaceOrCancelOrder"
    assert account.valid_until == 1804997046158
    assert account.create_time == 1789445050397
    assert account.active
    assert isinstance(reverse, DeepXDelegateWallets)
    assert reverse.delegate == DELEGATE
    assert reverse.wallets == [WALLET]
    assert [urlparse(path).path for path in delegate_api.paths] == [
        "/internal/v1/account/delegate-accounts",
        "/internal/v1/account/delegator-accounts",
    ]
    assert [parse_qs(urlparse(path).query) for path in delegate_api.paths] == [
        {"address": [WALLET]},
        {"address": [DELEGATE]},
    ]


@pytest.mark.parametrize("direction", ["forward", "reverse"])
def test_delegate_directory_rejects_duplicate_addresses(
    delegate_api: SimpleNamespace,
    direction: str,
) -> None:
    """Reject duplicate identities before returning a directory."""
    if direction == "forward":
        delegate_api.forward["data"].append(delegate_api.forward["data"][0].copy())
        query = get_deepx_delegate_accounts
        address = WALLET
    else:
        delegate_api.reverse["data"].append(WALLET.upper().replace("0X", "0x"))
        query = get_deepx_delegator_accounts
        address = DELEGATE

    with pytest.raises(ValueError, match="repeats"):
        query(address, delegate_api.config)


def test_delegate_directory_rejects_invalid_address_before_request(
    delegate_api: SimpleNamespace,
) -> None:
    """Reject a malformed wallet or delegate without contacting the server."""
    with pytest.raises(ValueError, match="20-byte hex wallet address"):
        get_deepx_delegate_accounts("invalid", delegate_api.config)
    with pytest.raises(ValueError, match="20-byte hex delegate address"):
        get_deepx_delegator_accounts("invalid", delegate_api.config)

    assert delegate_api.paths == []
