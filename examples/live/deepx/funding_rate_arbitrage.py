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
"""Run one DeepX testnet spot-long/perpetual-short funding cycle.

The observed funding rate is a sample, not a guaranteed payment or a payment schedule.
"""

import argparse
import os
import time
from decimal import Decimal
from typing import Any, Self

from nautilus_trader.adapters.deepx import (
    DeepXDataClientConfig,
    DeepXDataClientFactory,
    DeepXExecutionClientConfig,
    DeepXExecutionClientFactory,
    DeepXSubaccountSnapshot,
    get_deepx_perp_funding_fee_pages,
    get_deepx_perp_open_orders,
    get_deepx_perp_positions,
    get_deepx_spot_open_orders,
    get_deepx_wallet_account_snapshot,
)
from nautilus_trader.common import Environment
from nautilus_trader.config import LiveExecutionEngineConfig, StrategyConfig
from nautilus_trader.infrastructure import PostgresCacheConfig, PostgresConnectOptions
from nautilus_trader.live import LiveNode, LiveNodeHandle
from nautilus_trader.model import (
    AccountId,
    FundingRateUpdate,
    InstrumentId,
    OrderFilled,
    OrderSide,
    Quantity,
    QuoteTick,
    StrategyId,
    TimeInForce,
    TraderId,
)
from nautilus_trader.trading import Strategy

WALLET = "0x781ed35b167068c93dfadab41dfb680edaca4e50"
SPOT_ID = InstrumentId.from_str("ETH-USDC.DEEPX")
PERP_ID = InstrumentId.from_str("ETH-USDC-PERP.DEEPX")
NS_PER_SECOND = 1_000_000_000
RETRY_NS = 5 * NS_PER_SECOND
MAX_NO_FILL_ORDERS = 3
MAX_CYCLE_ORDERS = 12


def positive_decimal(value: str) -> Decimal:
    parsed = Decimal(value)
    if not parsed.is_finite() or parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a finite positive decimal")
    return parsed


def nonnegative_decimal(value: str) -> Decimal:
    parsed = Decimal(value)
    if not parsed.is_finite() or parsed < 0:
        raise argparse.ArgumentTypeError("value must be a finite nonnegative decimal")
    return parsed


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def meets_market_minimums(instrument: Any, amount: Decimal, price: Decimal) -> bool:
    """Check the venue's size and notional constraints without rounding."""
    minimum_quantity = instrument.min_quantity
    minimum_notional = instrument.min_notional
    return (
        amount > 0
        and amount % instrument.size_increment.as_decimal() == 0
        and (minimum_quantity is None or amount >= minimum_quantity.as_decimal())
        and (
            minimum_notional is None or amount * price >= minimum_notional.as_decimal()
        )
    )


class FundingArbitrageConfig(StrategyConfig):
    _CUSTOM_FIELDS = (
        "quantity",
        "min_funding_rate",
        "max_entry_basis_bps",
        "max_combined_notional",
        "max_age_seconds",
        "max_hold_seconds",
        "trade_enabled",
    )

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        for key in cls._CUSTOM_FIELDS:
            kwargs.pop(key, None)
        return super().__new__(cls, *args, **kwargs)

    def __init__(
        self,
        quantity: Decimal,
        min_funding_rate: Decimal,
        max_entry_basis_bps: Decimal,
        max_combined_notional: Decimal,
        max_age_seconds: int,
        max_hold_seconds: int,
        trade_enabled: bool,
        **_kwargs: Any,
    ) -> None:
        super().__init__()
        self.quantity = quantity
        self.min_funding_rate = min_funding_rate
        self.max_entry_basis_bps = max_entry_basis_bps
        self.max_combined_notional = max_combined_notional
        self.max_age_ns = max_age_seconds * NS_PER_SECOND
        self.max_hold_ns = max_hold_seconds * NS_PER_SECOND
        self.trade_enabled = trade_enabled


class FundingArbitrage(Strategy):
    """Buy spot, short the filled amount, then close the matched pair."""

    def __init__(self, config: FundingArbitrageConfig) -> None:
        super().__init__(config)
        self._config = config
        self._node_handle: LiveNodeHandle | None = None
        self._quotes: dict[InstrumentId, QuoteTick] = {}
        self._funding: FundingRateUpdate | None = None
        self._quantity: Quantity | None = None
        self._spot_qty = Decimal(0)
        self._perp_short_qty = Decimal(0)
        self._pending: tuple[str, Any] | None = None
        self._pending_filled = Decimal(0)
        self._no_fill_orders = 0
        self._orders_submitted = 0
        self._entry_started = False
        self._hedge_attempted = False
        self._exiting = False
        self._finished = False
        self._hold_start_ns: int | None = None
        self._last_attempt_ns: int | None = None

    def on_start(self) -> None:
        spot = self.cache.instrument(SPOT_ID)
        perp = self.cache.instrument(PERP_ID)
        if spot is None or perp is None:
            self.log.error("DeepX spot and perpetual instruments are required")
            self._stop_cycle()
            return
        if not self.portfolio.is_net_flat(SPOT_ID) or not self.portfolio.is_net_flat(
            PERP_ID
        ):
            self.log.error(
                "Existing exposure requires manual reconciliation before entry"
            )
            self._stop_cycle()
            return
        quantity = Quantity.from_decimal_dp(self._config.quantity, spot.size_precision)
        if (
            quantity.as_decimal() != self._config.quantity
            or self._config.quantity % spot.size_increment.as_decimal() != 0
            or self._config.quantity % perp.size_increment.as_decimal() != 0
            or spot.min_quantity is not None
            and self._config.quantity < spot.min_quantity.as_decimal()
            or perp.min_quantity is not None
            and self._config.quantity < perp.min_quantity.as_decimal()
        ):
            self.log.error(
                "Quantity must satisfy both DeepX market increments and minimums"
            )
            self._stop_cycle()
            return
        self._quantity = quantity
        self.subscribe_quotes(SPOT_ID)
        self.subscribe_quotes(PERP_ID)
        self.subscribe_funding_rates(PERP_ID)
        self.log.info(
            "Funding cycle ready; waiting for fresh positive funding and quotes"
        )

    def on_quote(self, quote: QuoteTick) -> None:
        if quote.instrument_id in (SPOT_ID, PERP_ID):
            self._quotes[quote.instrument_id] = quote
            self._advance()

    def on_funding_rate(self, funding_rate: FundingRateUpdate) -> None:
        if funding_rate.instrument_id == PERP_ID:
            self._funding = funding_rate
            self._advance()

    def on_order_filled(self, event: OrderFilled) -> None:
        if self._pending is None or event.client_order_id != self._pending[1]:
            return
        role = self._pending[0]
        amount = event.last_qty.as_decimal()
        self._pending_filled += amount
        if role == "spot_entry":
            self._spot_qty += amount
        elif role == "perp_entry":
            self._perp_short_qty += amount
        elif role == "perp_exit":
            self._perp_short_qty -= amount
        else:
            self._spot_qty -= amount
        self.log.info(
            f"{role} fill={amount} spot={self._spot_qty} perp_short={self._perp_short_qty}"
        )
        order = self.cache.order(event.client_order_id)
        if order is not None and order.is_closed:
            self._terminal(event)

    def on_order_canceled(self, event: Any) -> None:
        self._terminal(event)

    def on_order_expired(self, event: Any) -> None:
        self._terminal(event)

    def on_order_rejected(self, event: Any) -> None:
        self._terminal(event)

    def on_order_denied(self, event: Any) -> None:
        self._terminal(event)

    def _terminal(self, event: Any) -> None:
        if self._pending is None or event.client_order_id != self._pending[1]:
            return
        role = self._pending[0]
        self._pending = None
        self.log.info(f"{role} order reached terminal state")
        reason = getattr(event, "reason", None)
        if reason:
            self.log.warning(f"{role} order reason: {reason}")
        if self._pending_filled > 0:
            self._no_fill_orders = 0
        else:
            self._no_fill_orders += 1
        self._pending_filled = Decimal(0)
        if self._no_fill_orders >= MAX_NO_FILL_ORDERS:
            self._finished = True
            self.log.error(
                f"{MAX_NO_FILL_ORDERS} consecutive orders had no fill; "
                f"manual recovery required for spot={self._spot_qty}, "
                f"perp_short={self._perp_short_qty}"
            )
            self._stop_cycle()
            return
        self._advance()

    def _fresh(self, instrument_id: InstrumentId) -> QuoteTick | None:
        quote = self._quotes.get(instrument_id)
        if quote is None:
            return None
        age = self.clock.timestamp_ns() - quote.ts_event
        return quote if 0 <= age <= self._config.max_age_ns else None

    def _advance(self) -> None:
        if self._finished or self._pending is not None or self._quantity is None:
            return
        now = self.clock.timestamp_ns()
        if self._last_attempt_ns is not None and now - self._last_attempt_ns < RETRY_NS:
            return
        spot = self._fresh(SPOT_ID)
        perp = self._fresh(PERP_ID)
        if spot is None or perp is None:
            return
        if self._exiting:
            if self._spot_qty > self._perp_short_qty:
                self._submit("spot_exit", self._spot_qty - self._perp_short_qty, spot)
            elif self._perp_short_qty > 0:
                self._submit("perp_exit", self._perp_short_qty, perp)
            elif self._spot_qty == 0:
                self._finished = True
                self.log.info("Funding cycle completed flat")
                self._stop_cycle()
            return
        if self._entry_started:
            if self._spot_qty > self._perp_short_qty:
                if not self._hedge_attempted:
                    self._hedge_attempted = True
                    perp_instrument = self.cache.instrument(PERP_ID)
                    if perp_instrument is not None and meets_market_minimums(
                        perp_instrument,
                        self._spot_qty,
                        perp.bid_price.as_decimal(),
                    ):
                        self._submit("perp_entry", self._spot_qty, perp)
                    else:
                        self.log.warning(
                            "Spot fill is below perpetual order minimum; unwinding spot"
                        )
                        self._submit("spot_exit", self._spot_qty, spot)
                else:
                    self._submit(
                        "spot_exit", self._spot_qty - self._perp_short_qty, spot
                    )
            elif self._perp_short_qty > 0:
                if self._hold_start_ns is None:
                    self._hold_start_ns = now
                    self.log.info(
                        f"Hedged spot={self._spot_qty} short={self._perp_short_qty}"
                    )
                funding = self._funding
                if (
                    now - self._hold_start_ns >= self._config.max_hold_ns
                    or funding is not None
                    and funding.ts_event <= now
                    and now - funding.ts_event <= self._config.max_age_ns
                    and Decimal(str(funding.rate)) <= 0
                ):
                    self._exiting = True
                    self._advance()
            else:
                self._finished = True
                self.log.info("Entry did not fill; funding cycle ended flat")
                self._stop_cycle()
            return
        funding = self._funding
        if (
            funding is None
            or not 0 <= now - funding.ts_event <= self._config.max_age_ns
        ):
            return
        rate = Decimal(str(funding.rate))
        spot_ask = spot.ask_price.as_decimal()
        perp_bid = perp.bid_price.as_decimal()
        spot_instrument = self.cache.instrument(SPOT_ID)
        perp_instrument = self.cache.instrument(PERP_ID)
        if (
            rate <= self._config.min_funding_rate
            or spot_ask <= 0
            or perp_bid <= 0
            or spot_instrument is None
            or perp_instrument is None
            or not meets_market_minimums(
                spot_instrument, self._config.quantity, spot_ask
            )
            or not meets_market_minimums(
                perp_instrument, self._config.quantity, perp_bid
            )
            or spot.ask_size.as_decimal() < self._config.quantity
            or perp.bid_size.as_decimal() < self._config.quantity
            or perp_bid
            < spot_ask * (1 - self._config.max_entry_basis_bps / Decimal(10_000))
            or 2 * self._config.quantity * spot_ask > self._config.max_combined_notional
        ):
            return
        self.log.info(
            f"Positive funding sample={rate}; entry basis={perp_bid - spot_ask}"
        )
        if self._config.trade_enabled:
            self._entry_started = True
            self._submit("spot_entry", self._config.quantity, spot)
        else:
            self._last_attempt_ns = now

    def _submit(self, role: str, amount: Decimal, quote: QuoteTick) -> None:
        if self._orders_submitted >= MAX_CYCLE_ORDERS:
            self._finished = True
            self.log.error(
                f"Funding cycle reached {MAX_CYCLE_ORDERS} orders; "
                f"manual recovery required for spot={self._spot_qty}, "
                f"perp_short={self._perp_short_qty}"
            )
            self._stop_cycle()
            return
        instrument_id = SPOT_ID if role.startswith("spot") else PERP_ID
        instrument = self.cache.instrument(instrument_id)
        if instrument is None:
            self.log.error(f"Missing instrument during {role}")
            return
        quantity = Quantity.from_decimal_dp(amount, instrument.size_precision)
        side = OrderSide.BUY if role in ("spot_entry", "perp_exit") else OrderSide.SELL
        price = quote.ask_price if side == OrderSide.BUY else quote.bid_price
        if quantity.as_decimal() != amount or not meets_market_minimums(
            instrument, amount, price.as_decimal()
        ):
            self._finished = True
            self.log.error(
                f"Cannot submit {role} quantity {amount} at market precision"
            )
            self._stop_cycle()
            return
        if price.as_decimal() <= 0:
            return
        order = self.order_factory.limit(
            instrument_id=instrument_id,
            order_side=side,
            quantity=quantity,
            price=price,
            time_in_force=TimeInForce.IOC,
            reduce_only=role == "perp_exit",
        )
        self._pending = (role, order.client_order_id)
        self._pending_filled = Decimal(0)
        self._orders_submitted += 1
        self._last_attempt_ns = self.clock.timestamp_ns()
        self.log.info(f"Submitting {role} {quantity} at {price}")
        self.submit_order(order)

    def on_stop(self) -> None:
        if self._spot_qty or self._perp_short_qty:
            self.log.error(
                f"Residual exposure requires attention: spot={self._spot_qty}, "
                f"perp_short={self._perp_short_qty}"
            )

    def _stop_cycle(self) -> None:
        self.stop()
        if self._node_handle is not None:
            self._node_handle.stop()


def select_subaccount(
    requested: str,
    snapshots: list[DeepXSubaccountSnapshot],
    max_combined_notional: Decimal,
) -> None:
    account = next(
        (
            item
            for item in snapshots
            if item.profile.address.lower() == requested.lower()
        ),
        None,
    )
    if account is None or account.profile.status != "Active":
        raise ValueError("subaccount is not an active account of the specified wallet")
    if account.margin_ratio.exact_maintenance_margin_required is None:
        raise ValueError("subaccount has no exact margin observation")
    if Decimal(account.margin_ratio.exact_maintenance_margin_required) != 0:
        raise ValueError("subaccount has existing margin exposure")
    if Decimal(account.margin_ratio.margin_required) != 0:
        raise ValueError("subaccount has existing margin exposure")
    if any(Decimal(asset.balance_borrowed) != 0 for asset in account.balances.assets):
        raise ValueError("subaccount has outstanding borrowing")
    if any(
        asset.symbol == "ETH" and Decimal(asset.balance) != 0
        for asset in account.balances.assets
    ):
        raise ValueError("subaccount already owns ETH")
    usdc = next(
        (asset for asset in account.balances.assets if asset.symbol == "USDC"), None
    )
    if usdc is None or Decimal(usdc.balance) < max_combined_notional:
        raise ValueError("subaccount USDC balance is below the combined notional limit")


def verify_trade_result(subaccount: str, start_ms: int) -> Decimal:
    """Verify the account is flat and total settled short funding since startup."""
    snapshot = get_deepx_wallet_account_snapshot(WALLET)
    if subaccount.lower() not in [
        address.lower() for address in snapshot.directory.addresses
    ]:
        raise RuntimeError("subaccount is absent from the wallet after trading")
    account = next(
        (
            item
            for item in snapshot.subaccounts
            if item.profile.address.lower() == subaccount.lower()
        ),
        None,
    )
    if account is None:
        raise RuntimeError("subaccount snapshot is missing after trading")
    spot_orders = get_deepx_spot_open_orders(subaccount, name="ETH/USDC")
    perp_orders = get_deepx_perp_open_orders(subaccount, market_id=3)
    perp_positions = get_deepx_perp_positions(
        subaccount,
        market_id=3,
        only_closed=False,
        page_size=100,
    )
    eth_balance = next(
        (
            Decimal(asset.balance)
            for asset in account.balances.assets
            if asset.symbol == "ETH"
        ),
        None,
    )
    if (
        eth_balance is None
        or eth_balance != 0
        or account.margin_ratio.exact_maintenance_margin_required is None
        or Decimal(account.margin_ratio.exact_maintenance_margin_required) != 0
        or Decimal(account.margin_ratio.margin_required) != 0
        or spot_orders.items
        or spot_orders.has_next
        or perp_orders.items
        or perp_orders.has_next
        or perp_positions.has_next
        or any(position.status != "Closed" for position in perp_positions.items)
    ):
        raise RuntimeError(
            f"DeepX account is not verified flat: ETH={eth_balance}, "
            f"margin={account.margin_ratio.margin_required}, "
            f"spot_orders={len(spot_orders.items)}, perp_orders={len(perp_orders.items)}, "
            f"perp_positions={len(perp_positions.items)}"
        )
    pages = get_deepx_perp_funding_fee_pages(
        subaccount,
        max_pages=10,
        market_id=3,
        start_ms=start_ms,
    )
    settled_short_fees = [
        Decimal(record.fee)
        for page in pages
        for record in page.items
        if record.is_settled and not record.is_long
    ]
    net_funding = sum(settled_short_fees, Decimal(0))
    print(
        f"Verified flat account; settled short funding records={len(settled_short_fees)} "
        f"net={net_funding} USDC"
    )
    return net_funding


def postgres_options() -> tuple[PostgresConnectOptions, PostgresCacheConfig]:
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
            host=host, port=port, user=username, password=password, database=database
        ),
        PostgresCacheConfig(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--subaccount", help="wallet-owned DeepX subaccount for trading"
    )
    parser.add_argument(
        "--trade", action="store_true", help="submit one testnet funding cycle"
    )
    parser.add_argument("--quantity", type=positive_decimal, default=Decimal("0.001"))
    parser.add_argument(
        "--min-funding-rate", type=nonnegative_decimal, default=Decimal(0)
    )
    parser.add_argument(
        "--max-entry-basis-bps", type=nonnegative_decimal, default=Decimal(5)
    )
    parser.add_argument(
        "--max-combined-notional", type=positive_decimal, default=Decimal(5)
    )
    parser.add_argument("--max-age-seconds", type=positive_int, default=10)
    parser.add_argument("--max-hold-seconds", type=positive_int, default=3600)
    args = parser.parse_args()

    builder = (
        LiveNode.builder(
            "DEEPX-FUNDING-ARB-001", TraderId.from_str("TESTER-001"), Environment.LIVE
        )
        .with_shutdown_on_error(True)
        .add_data_client(
            None, DeepXDataClientFactory(), DeepXDataClientConfig(perpetual_only=False)
        )
    )
    if args.trade:
        if args.subaccount is None:
            parser.error("--subaccount is required with --trade")
        if not os.environ.get("DEEPX_TESTNET_PRIVATE_KEY"):
            parser.error("DEEPX_TESTNET_PRIVATE_KEY is required with --trade")
        snapshot = get_deepx_wallet_account_snapshot(WALLET)
        if args.subaccount.lower() not in [
            x.lower() for x in snapshot.directory.addresses
        ]:
            parser.error("subaccount is absent from the specified wallet directory")
        select_subaccount(
            args.subaccount, snapshot.subaccounts, args.max_combined_notional
        )
        spot_orders = get_deepx_spot_open_orders(args.subaccount, name="ETH/USDC")
        perp_orders = get_deepx_perp_open_orders(args.subaccount, market_id=3)
        if (
            spot_orders.items
            or spot_orders.has_next
            or perp_orders.items
            or perp_orders.has_next
        ):
            parser.error("subaccount has existing ETH spot or perpetual orders")
        database, cache_database = postgres_options()
        builder = (
            builder.with_cache_database_factory(cache_database)
            .with_timeout_connection(180)
            .with_exec_engine_config(LiveExecutionEngineConfig(reconciliation=True))
            .add_exec_client(
                None,
                DeepXExecutionClientFactory(),
                DeepXExecutionClientConfig(
                    account_id=AccountId.from_str("DEEPX-001"),
                    subaccount_id=args.subaccount,
                    postgres_cache_database_config=database,
                ),
            )
        )
    node = builder.build()
    strategy = FundingArbitrage(
        FundingArbitrageConfig(
            strategy_id=StrategyId.from_str("DEEPX-FUNDING-ARB-001"),
            external_order_claims=[SPOT_ID, PERP_ID],
            quantity=args.quantity,
            min_funding_rate=args.min_funding_rate,
            max_entry_basis_bps=args.max_entry_basis_bps,
            max_combined_notional=args.max_combined_notional,
            max_age_seconds=args.max_age_seconds,
            max_hold_seconds=args.max_hold_seconds,
            trade_enabled=args.trade,
        )
    )
    strategy._node_handle = node.handle()
    node.add_strategy(strategy)
    start_ms = time.time_ns() // 1_000_000
    node.run()
    if args.trade:
        verify_trade_result(args.subaccount, start_ms)


if __name__ == "__main__":
    main()
