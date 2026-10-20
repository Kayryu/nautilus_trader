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
"""Tests for the DeepX live execution example's preflight and order controls."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


EXAMPLE = Path(__file__).resolve().parents[5] / "examples/live/deepx/exec_tester.py"
SPEC = importlib.util.spec_from_file_location("deepx_exec_tester_example", EXAMPLE)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
select_subaccount = MODULE.select_subaccount
check_account_candidate = MODULE.check_account_candidate
_tester_config = MODULE.tester_config


ADDRESSES = [
    "0x2348ec3852b0d88495595d9309881651789e563e",
    "0xa4676ec9a5600686234b8d9d413d04bc2d8fda70",
]


def test_default_tester_cannot_submit_orders() -> None:
    """Leave every order trigger inactive until submission is explicitly requested."""
    config = _tester_config(False)

    assert config.dry_run
    assert config.open_position_on_start_qty is None
    assert not config.enable_limit_buys
    assert not config.enable_limit_sells
    assert not config.enable_stop_buys
    assert not config.enable_stop_sells
    assert not config.close_positions_on_stop


def test_order_mode_opens_only_the_ioc_scenario() -> None:
    """Keep limit and stop maintenance disabled in the explicit order scenario."""
    config = _tester_config(True)

    assert not config.dry_run
    assert str(config.open_position_on_start_qty) == "0.0010"
    assert str(config.open_position_time_in_force) == "IOC"
    assert config.open_position_on_first_quote
    assert config.close_positions_on_stop
    assert config.close_positions_qty_precision == 4
    assert str(config.close_positions_time_in_force) == "IOC"
    assert config.reduce_only_on_stop
    assert not config.enable_limit_buys
    assert not config.enable_limit_sells
    assert not config.enable_stop_buys
    assert not config.enable_stop_sells


def test_selects_only_a_wallet_owned_subaccount() -> None:
    """Return the public directory's canonical address for a matching selection."""
    assert select_subaccount(ADDRESSES[0].upper().replace("0X", "0x"), ADDRESSES) == ADDRESSES[0]


@pytest.mark.parametrize(
    ("requested", "addresses", "message"),
    [
        (None, [], "no DeepX subaccounts"),
        (None, ADDRESSES, "specify --subaccount"),
        ("0x1111111111111111111111111111111111111111", ADDRESSES, "absent"),
    ],
)
def test_rejects_missing_or_ambiguous_subaccount(
    requested: str | None,
    addresses: list[str],
    message: str,
) -> None:
    """Stop before node construction when directory ownership is unproven."""
    with pytest.raises(ValueError, match=message):
        select_subaccount(requested, addresses)


def test_account_candidate_rejects_observed_exposure() -> None:
    """Stop before execution startup when the public snapshot already shows exposure."""

    def account(
        status: str,
        margin: str,
        borrowed: str,
        exact_margin: str | None = "0",
    ) -> SimpleNamespace:
        return SimpleNamespace(
            profile=SimpleNamespace(address=ADDRESSES[0], status=status),
            margin_ratio=SimpleNamespace(
                margin_required=margin,
                exact_maintenance_margin_required=exact_margin,
            ),
            balances=SimpleNamespace(assets=[SimpleNamespace(balance_borrowed=borrowed)]),
        )

    check_account_candidate(ADDRESSES[0], [account("Active", "0", "0")])
    with pytest.raises(ValueError, match="nonzero margin"):
        check_account_candidate(ADDRESSES[0], [account("Active", "99.64", "0")])
    with pytest.raises(ValueError, match="no exact margin"):
        check_account_candidate(ADDRESSES[0], [account("Active", "0", "0", None)])
    with pytest.raises(ValueError, match="nonzero margin"):
        check_account_candidate(ADDRESSES[0], [account("Active", "0", "0", "0.00000001")])
    with pytest.raises(ValueError, match="outstanding borrowing"):
        check_account_candidate(ADDRESSES[0], [account("Active", "0", "1")])
    with pytest.raises(ValueError, match="not active"):
        check_account_candidate(ADDRESSES[0], [account("Frozen", "0", "0")])
    with pytest.raises(ValueError, match="absent"):
        check_account_candidate(ADDRESSES[1], [account("Active", "0", "0")])
