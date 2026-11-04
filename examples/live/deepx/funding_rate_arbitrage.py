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
import json
import os
import tempfile
import time
from decimal import ROUND_DOWN, ROUND_UP, Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
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
    BookType,
    FundingRateUpdate,
    InstrumentId,
    MarkPriceUpdate,
    OrderFilled,
    OrderSide,
    Price,
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
ORDER_TIMEOUT_NS = 15 * NS_PER_SECOND
ORDER_TIMER = "deepx-funding-order-timeout"
SETTLEMENT_TIMER = "deepx-funding-cancel-timeout"
HOLD_TIMER = "deepx-funding-hold-timeout"


class CycleState(str, Enum):
    FLAT = "flat"
    ENTERING = "entering"
    HEDGED = "hedged"
    HOLDING = "holding"
    EXITING = "exiting"
    SINGLE_LEG = "single_leg"
    COMPLETE = "complete"
    HALTED = "halted"


def contract_multiplier(instrument: Any) -> Decimal:
    value = instrument.multiplier.as_decimal()
    if value <= 0:
        raise ValueError("perpetual contract multiplier must be positive")
    return value


def contracts_for_base(base: Decimal, instrument: Any) -> Decimal:
    return base / contract_multiplier(instrument)


def entry_edge(
    base: Decimal,
    funding_rate: Decimal,
    mark: Decimal,
    spot_ask: Decimal,
    perp_bid: Decimal,
    spot_fee: Decimal,
    perp_fee: Decimal,
    slippage_bps: Decimal,
    holding_cost_bps: Decimal,
    exit_basis_risk_bps: Decimal,
) -> Decimal:
    """Conservative one-payment estimate in quote currency, before an order is sent."""
    gross = base * (funding_rate * mark + perp_bid - spot_ask)
    fees = base * 2 * (spot_ask * spot_fee + perp_bid * perp_fee)
    risk_bps = 4 * slippage_bps + holding_cost_bps + exit_basis_risk_bps
    return gross - fees - base * mark * risk_bps / Decimal(10_000)


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
        "max_slippage_bps",
        "holding_cost_bps",
        "exit_basis_risk_bps",
        "journal_path",
        "subaccount",
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
        max_slippage_bps: Decimal,
        holding_cost_bps: Decimal,
        exit_basis_risk_bps: Decimal,
        journal_path: Path | None,
        subaccount: str | None,
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
        self.max_slippage_bps = max_slippage_bps
        self.holding_cost_bps = holding_cost_bps
        self.exit_basis_risk_bps = exit_basis_risk_bps
        self.journal_path = journal_path
        self.subaccount = subaccount
        self.trade_enabled = trade_enabled


class FundingArbitrage(Strategy):
    """Buy spot, short the filled amount, then close the matched pair."""

    def __init__(self, config: FundingArbitrageConfig) -> None:
        super().__init__(config)
        self._config = config
        self._node_handle: LiveNodeHandle | None = None
        self._quotes: dict[InstrumentId, QuoteTick] = {}
        self._funding: FundingRateUpdate | None = None
        self._mark: MarkPriceUpdate | None = None
        self._depth: dict[InstrumentId, Any] = {}
        self._state = CycleState.FLAT
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
        quantity = Quantity.from_decimal_dp(self._config.quantity, spot.size_precision)
        perp_quantity = contracts_for_base(self._config.quantity, perp)
        if (
            quantity.as_decimal() != self._config.quantity
            or self._config.quantity % spot.size_increment.as_decimal() != 0
            or perp_quantity % perp.size_increment.as_decimal() != 0
            or spot.min_quantity is not None
            and self._config.quantity < spot.min_quantity.as_decimal()
            or perp.min_quantity is not None
            and perp_quantity < perp.min_quantity.as_decimal()
        ):
            self.log.error(
                "Quantity must satisfy both DeepX market increments and minimums"
            )
            self._stop_cycle()
            return
        self._quantity = quantity
        if self._config.trade_enabled and not self._recover():
            self._stop_cycle()
            return
        self.subscribe_quotes(SPOT_ID)
        self.subscribe_quotes(PERP_ID)
        self.subscribe_book_depth10(SPOT_ID, BookType.L2_MBP)
        self.subscribe_book_depth10(PERP_ID, BookType.L2_MBP)
        self.subscribe_funding_rates(PERP_ID)
        self.subscribe_mark_prices(PERP_ID)
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

    def on_mark_price(self, mark_price: MarkPriceUpdate) -> None:
        if mark_price.instrument_id == PERP_ID:
            self._mark = mark_price
            self._advance()

    def on_book_depth(self, depth: Any) -> None:
        if depth.instrument_id in (SPOT_ID, PERP_ID):
            self._depth[depth.instrument_id] = depth
            self._advance()

    def _persist(self) -> None:
        path = self._config.journal_path
        if path is None:
            return
        record = {
            "version": 1,
            "subaccount": self._config.subaccount,
            "state": self._state.value,
            "spot_base": str(self._spot_qty),
            "perp_contracts": str(self._perp_short_qty),
            "updated_ns": self.clock.timestamp_ns(),
        }
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=path.name + ".",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            json.dump(record, stream)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def _recover(self) -> bool:
        """Trust a prior cycle only when the durable record matches venue exposure."""
        path = self._config.journal_path
        subaccount = self._config.subaccount
        if path is None or subaccount is None:
            self.log.error("Trading requires a journal and subaccount")
            return False
        try:
            snapshot = get_deepx_wallet_account_snapshot(WALLET)
            account = next(
                x
                for x in snapshot.subaccounts
                if x.profile.address.lower() == subaccount.lower()
            )
            if account.profile.status != "Active" or any(
                Decimal(x.balance_borrowed) != 0 for x in account.balances.assets
            ):
                raise ValueError("subaccount inactive or has borrowed balances")
            spot_orders = get_deepx_spot_open_orders(subaccount, name="ETH/USDC")
            perp_orders = get_deepx_perp_open_orders(subaccount, market_id=3)
            positions = get_deepx_perp_positions(
                subaccount, market_id=3, only_closed=False, page_size=100
            )
            if (
                spot_orders.items
                or spot_orders.has_next
                or perp_orders.items
                or perp_orders.has_next
                or positions.has_next
                or self.cache.orders_open(instrument_id=SPOT_ID)
                or self.cache.orders_open(instrument_id=PERP_ID)
            ):
                raise ValueError(
                    "open orders or incomplete position pages require reconciliation"
                )
            spot = sum(
                (
                    Decimal(x.balance)
                    for x in account.balances.assets
                    if x.symbol == "ETH"
                ),
                Decimal(0),
            )
            short_base = sum(
                (
                    Decimal(x.base_asset_amount)
                    for x in positions.items
                    if x.status != "Closed" and not x.is_long
                ),
                Decimal(0),
            )
            if any(x.status != "Closed" and x.is_long for x in positions.items):
                raise ValueError("unexpected long perpetual position")
            multiplier = contract_multiplier(self.cache.instrument(PERP_ID))
            short_contracts = short_base / multiplier
            if path.exists():
                record = json.loads(path.read_text(encoding="utf-8"))
                if (
                    record["version"] != 1
                    or record["subaccount"].lower() != subaccount.lower()
                ):
                    raise ValueError("journal identity mismatch")
                self._state = CycleState(record["state"])
                self._spot_qty = Decimal(record["spot_base"])
                self._perp_short_qty = Decimal(record["perp_contracts"])
                if any(
                    not value.is_finite() or value < 0
                    for value in (self._spot_qty, self._perp_short_qty)
                ):
                    raise ValueError("invalid journal exposure")
                if self._spot_qty != spot or self._perp_short_qty != short_contracts:
                    raise ValueError("journal and venue exposure disagree")
                if self._state in (CycleState.COMPLETE, CycleState.HALTED):
                    raise ValueError("prior cycle is complete or halted")
                if spot or short_contracts:
                    self._entry_started = True
                    self._hedge_attempted = True
                    self._exiting = True
                    self._state = (
                        CycleState.SINGLE_LEG
                        if spot != short_base
                        else CycleState.EXITING
                    )
                    self._persist()
                elif self._state != CycleState.FLAT:
                    raise ValueError(
                        "prior cycle ended flat; new entry requires a new journal"
                    )
            elif spot or short_contracts:
                raise ValueError("unrecorded venue exposure")
            else:
                self._persist()
        except (OSError, ValueError, InvalidOperation, KeyError, StopIteration) as e:
            self._state = CycleState.HALTED
            self.log.error(f"Recovery stopped: {e}")
            return False
        return True

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
        self._state = (
            CycleState.SINGLE_LEG
            if self._spot_qty
            != self._perp_short_qty
            * contract_multiplier(self.cache.instrument(PERP_ID))
            else CycleState.HEDGED
        )
        self._persist()
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
        self.clock.cancel_timer(ORDER_TIMER)
        self.clock.cancel_timer(SETTLEMENT_TIMER)
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
            self._state = CycleState.HALTED
            self._persist()
            self.log.error(
                f"{MAX_NO_FILL_ORDERS} consecutive orders had no fill; "
                f"manual recovery required for spot={self._spot_qty}, "
                f"perp_short={self._perp_short_qty}"
            )
            self._stop_cycle()
            return
        self._advance()

    def _order_timeout(self, order_id: Any) -> None:
        if self._pending is None or self._pending[1] != order_id:
            return
        order = self.cache.order(order_id)
        if order is None:
            self._state = CycleState.HALTED
            self._persist()
            self._stop_cycle()
            return
        if order.is_closed:
            self._terminal(order)
            return
        self.log.warning(f"Order {order_id} timed out; requesting cancellation")
        self.cancel_order(order)
        self.clock.set_time_alert_ns(
            SETTLEMENT_TIMER,
            self.clock.timestamp_ns() + ORDER_TIMEOUT_NS,
            lambda _event: self._cancel_timeout(order_id),
        )

    def _cancel_timeout(self, order_id: Any) -> None:
        if self._pending is not None and self._pending[1] == order_id:
            self._state = CycleState.HALTED
            self._persist()
            self.log.error(
                "Cancellation unconfirmed; manual order reconciliation required"
            )
            self._stop_cycle()

    def _hold_timeout(self) -> None:
        if self._finished or self._exiting or self._hold_start_ns is None:
            return
        self._exiting = True
        self._state = CycleState.EXITING
        self._persist()
        self._advance()

    def _depth_available(
        self, instrument_id: InstrumentId, side: OrderSide, limit: Decimal
    ) -> Decimal:
        depth = self._depth.get(instrument_id)
        if (
            depth is None
            or not 0
            <= self.clock.timestamp_ns() - depth.ts_event
            <= self._config.max_age_ns
        ):
            return Decimal(0)
        levels = depth.asks if side == OrderSide.BUY else depth.bids
        return sum(
            (
                level.size.as_decimal()
                for level in levels
                if (
                    level.price.as_decimal() <= limit
                    if side == OrderSide.BUY
                    else level.price.as_decimal() >= limit
                )
            ),
            Decimal(0),
        )

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
        perp_instrument = self.cache.instrument(PERP_ID)
        if perp_instrument is None:
            return
        multiplier = contract_multiplier(perp_instrument)
        short_base = self._perp_short_qty * multiplier
        if self._exiting:
            if self._spot_qty > short_base:
                self._state = CycleState.SINGLE_LEG
                self._submit("spot_exit", self._spot_qty - short_base, spot)
            elif self._perp_short_qty > 0:
                self._state = CycleState.EXITING
                self._submit("perp_exit", self._perp_short_qty, perp)
            elif self._spot_qty == 0:
                self._finished = True
                self._state = CycleState.COMPLETE
                self._persist()
                self.log.info("Funding cycle completed flat")
                self._stop_cycle()
            return
        if self._entry_started:
            if self._spot_qty > short_base:
                if not self._hedge_attempted:
                    self._hedge_attempted = True
                    contracts = contracts_for_base(self._spot_qty, perp_instrument)
                    if meets_market_minimums(
                        perp_instrument,
                        contracts,
                        perp.bid_price.as_decimal(),
                    ):
                        self._submit("perp_entry", contracts, perp)
                    else:
                        self.log.warning(
                            "Spot fill is below perpetual order minimum; unwinding spot"
                        )
                        self._submit("spot_exit", self._spot_qty, spot)
                else:
                    self._submit("spot_exit", self._spot_qty - short_base, spot)
            elif self._perp_short_qty > 0:
                if self._hold_start_ns is None:
                    self._hold_start_ns = now
                    self._state = CycleState.HOLDING
                    self._persist()
                    self.clock.set_time_alert_ns(
                        HOLD_TIMER,
                        now + self._config.max_hold_ns,
                        lambda _event: self._hold_timeout(),
                    )
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
                    self._state = CycleState.EXITING
                    self.clock.cancel_timer(HOLD_TIMER)
                    self._persist()
                    self._advance()
            else:
                self._finished = True
                self._state = CycleState.COMPLETE
                self._persist()
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
        mark = self._mark
        if mark is None or not 0 <= now - mark.ts_event <= self._config.max_age_ns:
            return
        mark_price = mark.value.as_decimal()
        spot_ask = spot.ask_price.as_decimal()
        perp_bid = perp.bid_price.as_decimal()
        spot_instrument = self.cache.instrument(SPOT_ID)
        contracts = contracts_for_base(self._config.quantity, perp_instrument)
        slippage = self._config.max_slippage_bps / Decimal(10_000)
        if (
            rate <= self._config.min_funding_rate
            or spot_ask <= 0
            or perp_bid <= 0
            or mark_price <= 0
            or spot_instrument is None
            or not meets_market_minimums(
                spot_instrument, self._config.quantity, spot_ask
            )
            or not meets_market_minimums(perp_instrument, contracts, perp_bid)
            or self._depth_available(SPOT_ID, OrderSide.BUY, spot_ask * (1 + slippage))
            < self._config.quantity
            or self._depth_available(PERP_ID, OrderSide.SELL, perp_bid * (1 - slippage))
            < contracts
            or perp_bid
            < spot_ask * (1 - self._config.max_entry_basis_bps / Decimal(10_000))
            or self._config.quantity * (spot_ask + perp_bid)
            > self._config.max_combined_notional
            or entry_edge(
                self._config.quantity,
                rate,
                mark_price,
                spot_ask,
                perp_bid,
                spot_instrument.taker_fee,
                perp_instrument.taker_fee,
                self._config.max_slippage_bps,
                self._config.holding_cost_bps,
                self._config.exit_basis_risk_bps,
            )
            <= 0
        ):
            return
        self.log.info(
            f"Positive funding sample={rate}; entry basis={perp_bid - spot_ask}"
        )
        if self._config.trade_enabled:
            self._entry_started = True
            self._state = CycleState.ENTERING
            self._persist()
            self._submit("spot_entry", self._config.quantity, spot)
        else:
            self._last_attempt_ns = now

    def _submit(self, role: str, amount: Decimal, quote: QuoteTick) -> None:
        if self._orders_submitted >= MAX_CYCLE_ORDERS:
            self._finished = True
            self._state = CycleState.HALTED
            self._persist()
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
        side = OrderSide.BUY if role in ("spot_entry", "perp_exit") else OrderSide.SELL
        reference = quote.ask_price if side == OrderSide.BUY else quote.bid_price
        slippage = self._config.max_slippage_bps / Decimal(10_000)
        increment = instrument.price_increment.as_decimal()
        raw_limit = reference.as_decimal() * (
            1 + slippage if side == OrderSide.BUY else 1 - slippage
        )
        rounding = ROUND_DOWN if side == OrderSide.BUY else ROUND_UP
        limit = (raw_limit / increment).to_integral_value(rounding=rounding) * increment
        price = Price.from_decimal_dp(limit, instrument.price_precision)
        if role == "spot_exit":
            account = self.portfolio.account(AccountId.from_str("DEEPX-001"))
            free = account.balance_free(instrument.base_currency) if account else None
            if free is None:
                self.log.error("No reconciled free spot inventory for exit")
                self._state = CycleState.HALTED
                self._persist()
                self._stop_cycle()
                return
            amount = min(amount, free.as_decimal())
            amount = (
                amount / instrument.size_increment.as_decimal()
            ).to_integral_value(
                rounding=ROUND_DOWN
            ) * instrument.size_increment.as_decimal()
        quantity = Quantity.from_decimal_dp(amount, instrument.size_precision)
        if quantity.as_decimal() != amount or not meets_market_minimums(
            instrument, amount, price.as_decimal()
        ):
            self._finished = True
            self._state = CycleState.HALTED
            self._persist()
            self.log.error(
                f"Cannot submit {role} quantity {amount} at market precision"
            )
            self._stop_cycle()
            return
        if price.as_decimal() <= 0:
            return
        order = self.order_factory.limit(
            **{
                "instrument_id": instrument_id,
                "order_side": side,
                "quantity": quantity,
                "price": price,
                "time_in_force": TimeInForce.IOC,
                **({"reduce_only": True} if role == "perp_exit" else {}),
            },
        )
        self._pending = (role, order.client_order_id)
        self._pending_filled = Decimal(0)
        self._orders_submitted += 1
        self._last_attempt_ns = self.clock.timestamp_ns()
        self.log.info(f"Submitting {role} {quantity} at {price}")
        self.submit_order(order)
        self.clock.set_time_alert_ns(
            ORDER_TIMER,
            self.clock.timestamp_ns() + ORDER_TIMEOUT_NS,
            lambda _event: self._order_timeout(order.client_order_id),
        )

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
    # if any(
    #     asset.symbol == "ETH" and Decimal(asset.balance) != 0
    #     for asset in account.balances.assets
    # ):
    #     raise ValueError("subaccount already owns ETH")
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
    parser.add_argument(
        "--max-slippage-bps", type=nonnegative_decimal, default=Decimal(2)
    )
    parser.add_argument(
        "--holding-cost-bps", type=nonnegative_decimal, default=Decimal(1)
    )
    parser.add_argument(
        "--exit-basis-risk-bps", type=nonnegative_decimal, default=Decimal(5)
    )
    parser.add_argument(
        "--journal", type=Path, help="durable record for one funding cycle"
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
        if args.journal is None:
            parser.error("--journal is required with --trade")
        if not args.journal.parent.is_dir():
            parser.error("journal parent directory must exist")
        if not os.environ.get("DEEPX_TESTNET_PRIVATE_KEY"):
            parser.error("DEEPX_TESTNET_PRIVATE_KEY is required with --trade")
        snapshot = get_deepx_wallet_account_snapshot(WALLET)
        if args.subaccount.lower() not in [
            x.lower() for x in snapshot.directory.addresses
        ]:
            parser.error("subaccount is absent from the specified wallet directory")
        if not args.journal.exists():
            select_subaccount(
                args.subaccount, snapshot.subaccounts, args.max_combined_notional
            )
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
            max_slippage_bps=args.max_slippage_bps,
            holding_cost_bps=args.holding_cost_bps,
            exit_basis_risk_bps=args.exit_basis_risk_bps,
            journal_path=args.journal,
            subaccount=args.subaccount,
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
