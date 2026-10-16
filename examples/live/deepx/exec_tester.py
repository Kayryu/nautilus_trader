#!/usr/bin/env python3
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
"""Connect a DeepX testnet execution client and optionally test one IOC order."""

import argparse
import os
from decimal import Decimal

from nautilus_trader.adapters.deepx import (
    DEEPX,
    DeepXDataClientConfig,
    DeepXDataClientFactory,
    DeepXExecutionClientConfig,
    DeepXExecutionClientFactory,
    DeepXSubaccountSnapshot,
    get_deepx_wallet_account_snapshot,
)
from nautilus_trader.common import Environment
from nautilus_trader.infrastructure import PostgresCacheConfig, PostgresConnectOptions
from nautilus_trader.live import LiveNode
from nautilus_trader.model import (
    AccountId,
    ClientId,
    InstrumentId,
    Quantity,
    StrategyId,
    TimeInForce,
    TraderId,
)
from nautilus_trader.testkit import ExecTesterConfig

WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
INSTRUMENT_ID = InstrumentId.from_str("ETH-USDC-PERP.DEEPX")
ORDER_QTY = "0.0010"


def select_subaccount(requested: str | None, addresses: list[str]) -> str:
    """Select one address only when the public wallet directory proves ownership."""
    if not addresses:
        raise ValueError("the specified wallet currently has no DeepX subaccounts")
    if requested is not None:
        for address in addresses:
            if address.lower() == requested.lower():
                return address
        raise ValueError("subaccount is absent from the specified wallet directory")
    if len(addresses) != 1:
        raise ValueError(
            "specify --subaccount when the wallet has zero or multiple subaccounts"
        )
    return addresses[0]


def check_account_candidate(
    subaccount: str,
    snapshots: list[DeepXSubaccountSnapshot],
) -> None:
    """Reject account observations that already contradict flat-account startup."""
    snapshot = next(
        (
            item
            for item in snapshots
            if item.profile.address.lower() == subaccount.lower()
        ),
        None,
    )
    if snapshot is None:
        raise ValueError(
            "selected subaccount is absent from the wallet account snapshot"
        )
    if snapshot.profile.status != "Active":
        raise ValueError("selected subaccount is not active")
    if Decimal(snapshot.margin_ratio.margin_required) != 0:
        raise ValueError("selected subaccount has a nonzero margin requirement")
    if any(Decimal(asset.balance_borrowed) != 0 for asset in snapshot.balances.assets):
        raise ValueError("selected subaccount has outstanding borrowing")


def postgres_options() -> tuple[PostgresConnectOptions, PostgresCacheConfig]:
    """Read the required durable transaction store settings from the environment."""
    required = ("POSTGRES_USERNAME", "POSTGRES_PASSWORD", "POSTGRES_DATABASE")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError(f"missing PostgreSQL settings: {', '.join(missing)}")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    username = os.environ["POSTGRES_USERNAME"]
    password = os.environ["POSTGRES_PASSWORD"]
    database = os.environ["POSTGRES_DATABASE"]
    return (
        PostgresConnectOptions(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database,
        ),
        PostgresCacheConfig(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        ),
    )


def tester_config(submit_order: bool) -> ExecTesterConfig:
    """Limit execution testing to one optional IOC open and shutdown close."""
    return ExecTesterConfig(
        strategy_id=StrategyId.from_str("DEEPX-EXEC-TESTER-001"),
        instrument_id=INSTRUMENT_ID,
        client_id=ClientId.from_str(DEEPX),
        external_order_claims=[INSTRUMENT_ID],
        order_qty=Quantity.from_str(ORDER_QTY),
        subscribe_quotes=True,
        subscribe_trades=True,
        open_position_on_start_qty=Decimal(ORDER_QTY) if submit_order else None,
        open_position_on_first_quote=True,
        open_position_time_in_force=TimeInForce.IOC,
        enable_limit_buys=False,
        enable_limit_sells=False,
        enable_stop_buys=False,
        enable_stop_sells=False,
        cancel_orders_on_stop=False,
        close_positions_on_stop=submit_order,
        close_positions_qty_precision=4,
        close_positions_time_in_force=TimeInForce.IOC,
        reduce_only_on_stop=True,
        dry_run=not submit_order,
        log_data=True,
    )


def main() -> None:
    """Connect the account, and submit only when the explicit order flag is present."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--subaccount", help="AccountId20 from the specified wallet directory"
    )
    parser.add_argument(
        "--submit-order",
        action="store_true",
        help="submit one ETH-USDC perpetual IOC market buy and close on shutdown",
    )
    args = parser.parse_args()

    snapshot = get_deepx_wallet_account_snapshot(WALLET)
    subaccount = select_subaccount(args.subaccount, snapshot.directory.addresses)
    check_account_candidate(subaccount, snapshot.subaccounts)
    if not os.environ.get("DEEPX_TESTNET_PRIVATE_KEY"):
        parser.error("DEEPX_TESTNET_PRIVATE_KEY is required for execution startup")
    database, cache_database = postgres_options()

    node = (
        LiveNode.builder(
            "DEEPX-EXEC-TESTER-001",
            TraderId.from_str("TESTER-001"),
            Environment.LIVE,
        )
        .with_cache_database_factory(cache_database)
        .with_timeout_connection(180)
        .with_reconciliation(reconciliation=True)
        .add_data_client(
            None,
            DeepXDataClientFactory(),
            DeepXDataClientConfig(perpetual_only=True),
        )
        .add_exec_client(
            None,
            DeepXExecutionClientFactory(),
            DeepXExecutionClientConfig(
                account_id=AccountId.from_str("DEEPX-001"),
                subaccount_id=subaccount,
                postgres_cache_database_config=database,
            ),
        )
        .build()
    )
    node.add_builtin_strategy(
        "ExecTester",
        tester_config(args.submit_order),
    )
    node.run()


if __name__ == "__main__":
    main()
