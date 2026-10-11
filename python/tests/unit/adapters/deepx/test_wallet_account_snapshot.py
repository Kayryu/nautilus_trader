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

"""Tests for read-only DeepX Python wallet account observations."""

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
from nautilus_trader.adapters.deepx import DeepXPerpLiquidationPrice
from nautilus_trader.adapters.deepx import DeepXSubaccountBalanceAsset
from nautilus_trader.adapters.deepx import DeepXSubaccountBalances
from nautilus_trader.adapters.deepx import DeepXSubaccountEquity
from nautilus_trader.adapters.deepx import DeepXSubaccountMarginRatio
from nautilus_trader.adapters.deepx import DeepXSubaccountProfile
from nautilus_trader.adapters.deepx import DeepXSubaccountSnapshot
from nautilus_trader.adapters.deepx import DeepXUserStats
from nautilus_trader.adapters.deepx import DeepXWalletAccountSnapshot
from nautilus_trader.adapters.deepx import DeepXWalletSubaccounts
from nautilus_trader.adapters.deepx import get_deepx_perp_liquidation_price
from nautilus_trader.adapters.deepx import get_deepx_user_stats
from nautilus_trader.adapters.deepx import get_deepx_wallet_account_snapshot
from nautilus_trader.adapters.deepx import get_deepx_wallet_subaccounts


WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
SUBACCOUNT = "0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc"
FIXTURE_DIR = Path(__file__).resolve().parents[5] / "crates/adapters/deepx/test_data/http/testnet"


@pytest.fixture
def account_api() -> Iterator[SimpleNamespace]:
    """Serve captured account payloads under one wallet-scoped directory."""
    directory = json.loads((FIXTURE_DIR / "wallet_subaccounts.json").read_bytes())
    directory["data"] = [SUBACCOUNT]
    responses = {
        "/internal/v1/account/subaccounts": directory,
        "/internal/v1/account/user-stats": json.loads(
            (FIXTURE_DIR / "wallet_user_stats.json").read_bytes(),
        ),
        "/internal/v1/account/perp/liquidate-price": json.loads(
            (FIXTURE_DIR / "perp_liquidation_price_account_market_3.json").read_bytes(),
        ),
        "/internal/v1/account/subaccount-info": json.loads(
            (FIXTURE_DIR / "subaccount_info.json").read_bytes(),
        ),
        "/internal/v1/account/balances": json.loads(
            (FIXTURE_DIR / "subaccount_balances.json").read_bytes(),
        ),
        "/internal/v1/account/equity": json.loads(
            (FIXTURE_DIR / "subaccount_equity.json").read_bytes(),
        ),
        "/internal/v1/account/margin-ratio": json.loads(
            (FIXTURE_DIR / "subaccount_margin_ratio_exact.json").read_bytes(),
        ),
    }
    responses["/internal/v1/account/user-stats"]["data"]["subaccounts"] = [SUBACCOUNT]
    responses["/internal/v1/account/user-stats"]["data"]["numberOfSubAccounts"] = 1
    state = SimpleNamespace(responses=responses, paths=[], directory_reads=0, final_directory=None)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.paths.append(self.path)
            path = urlparse(self.path).path
            payload = state.responses[path]
            if path == "/internal/v1/account/subaccounts":
                state.directory_reads += 1
                if state.directory_reads > 1 and state.final_directory is not None:
                    payload = state.final_directory
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
    state.config = DeepXDataClientConfig(
        network=DeepXNetworkConfig(base_url_rest=f"http://127.0.0.1:{server.server_port}"),
    )
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_wallet_subaccount_directory(account_api: SimpleNamespace) -> None:
    """Return a request-scoped directory without additional account calls."""
    directory = get_deepx_wallet_subaccounts(WALLET, account_api.config)

    assert isinstance(directory, DeepXWalletSubaccounts)
    assert directory.wallet == WALLET
    assert directory.addresses == [SUBACCOUNT]
    assert len(account_api.paths) == 1
    assert parse_qs(urlparse(account_api.paths[0]).query) == {"address": [WALLET]}


def test_user_stats_preserves_exact_amount_and_directory(account_api: SimpleNamespace) -> None:
    """Expose validated counters and the uninterpreted exact staked amount."""
    stats = get_deepx_user_stats(WALLET, account_api.config)

    assert isinstance(stats, DeepXUserStats)
    assert len(stats.subaccounts) == 1
    assert stats.subaccounts[0] == SUBACCOUNT
    assert stats.if_staked_quote_asset_amount == "0"
    assert stats.number_of_sub_accounts == 1
    assert stats.number_of_sub_accounts_created == 4
    assert [urlparse(path).path for path in account_api.paths] == [
        "/internal/v1/account/user-stats",
    ]
    assert parse_qs(urlparse(account_api.paths[0]).query) == {"address": [WALLET]}


def test_user_stats_rejects_inconsistent_count(account_api: SimpleNamespace) -> None:
    """Reject an inconsistent venue response without returning partial data."""
    account_api.responses["/internal/v1/account/user-stats"]["data"]["numberOfSubAccounts"] = 3

    with pytest.raises(ValueError, match="subaccount counters are inconsistent"):
        get_deepx_user_stats(WALLET, account_api.config)


def test_perp_liquidation_price_preserves_nullable_value(account_api: SimpleNamespace) -> None:
    """Expose the validated market identity and nullable venue observation."""
    price = get_deepx_perp_liquidation_price(
        SUBACCOUNT,
        market_id=3,
        config=account_api.config,
    )

    assert isinstance(price, DeepXPerpLiquidationPrice)
    assert price.address == SUBACCOUNT
    assert price.market_id == 3
    assert price.market_name == "ETH-USDC"
    assert price.liquidate_price is None
    assert [urlparse(path).path for path in account_api.paths] == [
        "/internal/v1/account/perp/liquidate-price",
    ]
    assert parse_qs(urlparse(account_api.paths[0]).query) == {
        "address": [SUBACCOUNT],
        "marketId": ["3"],
    }


def test_perp_liquidation_price_preserves_exact_decimal(account_api: SimpleNamespace) -> None:
    """Keep every price digit supplied by a validated response."""
    account_api.responses["/internal/v1/account/perp/liquidate-price"]["data"]["liquidatePrice"] = (
        "1234.567890123456789"
    )

    price = get_deepx_perp_liquidation_price(
        SUBACCOUNT,
        market_name="ETH-USDC",
        config=account_api.config,
    )

    assert price.liquidate_price == "1234.567890123456789"
    assert parse_qs(urlparse(account_api.paths[0]).query) == {
        "address": [SUBACCOUNT],
        "name": ["ETH-USDC"],
    }


def test_perp_liquidation_price_requires_one_market_selector(
    account_api: SimpleNamespace,
) -> None:
    """Reject ambiguous markets before contacting the venue."""
    with pytest.raises(ValueError, match="exactly one market_name or market_id"):
        get_deepx_perp_liquidation_price(
            SUBACCOUNT,
            market_id=3,
            market_name="ETH-USDC",
            config=account_api.config,
        )

    assert account_api.paths == []


def test_wallet_account_snapshot_preserves_exact_validated_values(
    account_api: SimpleNamespace,
) -> None:
    """Expose every validated response and preserve the directory order."""
    snapshot = get_deepx_wallet_account_snapshot(WALLET, account_api.config)

    assert isinstance(snapshot, DeepXWalletAccountSnapshot)
    assert snapshot.directory.wallet == WALLET
    assert snapshot.directory.addresses == [SUBACCOUNT]
    assert snapshot.stats.subaccounts == [SUBACCOUNT]
    assert snapshot.stats.number_of_sub_accounts_created == 4
    assert len(snapshot.subaccounts) == 1
    account = snapshot.subaccounts[0]
    assert isinstance(account, DeepXSubaccountSnapshot)
    assert isinstance(account.profile, DeepXSubaccountProfile)
    assert account.profile.authority == WALLET
    assert account.profile.address == SUBACCOUNT
    assert account.profile.status == "Active"
    assert account.profile.spot_positions_json == []
    assert not account.profile.spot_margin_trading_enabled
    assert account.profile.margin_strategy == "Cross"
    assert isinstance(account.balances, DeepXSubaccountBalances)
    assert account.balances.address == SUBACCOUNT
    usdc = account.balances.assets[0]
    assert isinstance(usdc, DeepXSubaccountBalanceAsset)
    assert usdc.symbol == "USDC"
    assert usdc.decimals == 6
    assert usdc.balance == "999.783258"
    assert usdc.balance_usd == "999.78"
    assert usdc.balance_borrowed == "0.0"
    assert isinstance(account.equity, DeepXSubaccountEquity)
    assert account.equity.total_deposits_usd == "999.78"
    assert account.equity.unrealized_pnl_usd == "-2.73"
    assert account.equity.equity_usd == "997.05"
    assert isinstance(account.margin_ratio, DeepXSubaccountMarginRatio)
    assert account.margin_ratio.collateral == "970.96"
    assert account.margin_ratio.margin_required == "0.0"
    assert account.margin_ratio.margin_ratio is None
    assert account.margin_ratio.exact_total_collateral == "970.964181"
    assert account.margin_ratio.exact_maintenance_margin_required == "0"
    assert account.margin_ratio.exact_uni_mmr is None
    assert [urlparse(path).path for path in account_api.paths] == [
        "/internal/v1/account/subaccounts",
        "/internal/v1/account/user-stats",
        "/internal/v1/account/subaccount-info",
        "/internal/v1/account/balances",
        "/internal/v1/account/equity",
        "/internal/v1/account/margin-ratio",
        "/internal/v1/account/subaccounts",
    ]
    assert [parse_qs(urlparse(path).query) for path in account_api.paths] == [
        {"address": [WALLET]},
        {"address": [WALLET]},
        {"address": [SUBACCOUNT]},
        {"subaccount": [SUBACCOUNT]},
        {"address": [SUBACCOUNT]},
        {"address": [SUBACCOUNT]},
        {"address": [WALLET]},
    ]


def test_wallet_account_snapshot_rejects_changed_directory(
    account_api: SimpleNamespace,
) -> None:
    """Reject a wallet whose subaccount membership changes during collection."""
    account_api.final_directory = {
        "code": 200,
        "msg": "success",
        "data": [],
        "fail": False,
    }

    with pytest.raises(ValueError, match="wallet directory changed during account collection"):
        get_deepx_wallet_account_snapshot(WALLET, account_api.config)

    assert account_api.directory_reads == 2


def test_wallet_account_snapshot_rejects_foreign_profile(account_api: SimpleNamespace) -> None:
    """Reject the whole wallet read when a subaccount has another authority."""
    account_api.responses["/internal/v1/account/subaccount-info"]["data"]["authority"] = (
        "0x1111111111111111111111111111111111111111"
    )

    with pytest.raises(ValueError, match="authority does not match"):
        get_deepx_wallet_account_snapshot(WALLET, account_api.config)

    assert len(account_api.paths) == 3


def test_wallet_account_snapshot_rejects_directory_stats_mismatch(
    account_api: SimpleNamespace,
) -> None:
    """Reject divergent directory endpoints before any subaccount reads."""
    account_api.responses["/internal/v1/account/user-stats"]["data"]["subaccounts"] = [
        "0x1111111111111111111111111111111111111111",
    ]

    with pytest.raises(ValueError, match="user stats subaccounts differ from wallet directory"):
        get_deepx_wallet_account_snapshot(WALLET, account_api.config)

    assert [urlparse(path).path for path in account_api.paths] == [
        "/internal/v1/account/subaccounts",
        "/internal/v1/account/user-stats",
    ]


def test_wallet_account_snapshot_rejects_invalid_wallet_before_request(
    account_api: SimpleNamespace,
) -> None:
    """Reject invalid identities without contacting the venue."""
    with pytest.raises(ValueError, match="20-byte hex wallet address"):
        get_deepx_wallet_account_snapshot("invalid", account_api.config)

    assert account_api.paths == []
